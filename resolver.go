package kuberesolver

import (
	"context"
	"fmt"
	"net"

	wrr "google.golang.org/grpc/balancer/weightedroundrobin"
	"google.golang.org/grpc/resolver"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type KubeResolver struct {
	Service types.NamespacedName

	ServicePortName string
	ServicePort     uint32

	grpcSecurityProtocol string

	runtimeManager manager.Manager

	preferredZoneWeights map[string]int

	clientConn resolver.ClientConn

	closeFunc func()

	serviceListOptions []client.ListOption
}

// DefaultPortName is used for targets when no port name or number suffix is set on the `kube:///service[:port]` target
// and the resolver wasn't built with either of WithDefaultPortName or WithDefaultPort.
const DefaultPortName = "grpc"

var _ resolver.Resolver = &KubeResolver{}

func (r *KubeResolver) Close() {
	r.closeFunc()
}

// ResolveNow triggers a syncronous reconcile.
// The caching logic in the kube client will prevent this operation from overloading the API
// and makes this concurrency safe
func (r *KubeResolver) ResolveNow(resolver.ResolveNowOptions) {

	_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: r.Service.Namespace}})
	if err != nil {
		r.clientConn.ReportError(err)
	}
}

func (r *KubeResolver) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {

	reconcileLog := log.FromContext(ctx)
	k8sClient := r.runtimeManager.GetClient()

	reconcileLog.V(1).Info("Starting reconcile")

	if r.Service.Namespace != req.Namespace {
		reconcileLog.V(0).Info("Reconcile requested on resource outside target namespace, this shouldn't happen",
			"serviceNamespace", r.Service.Namespace)
		return reconcile.Result{}, nil
	}

	endpointSliceList := new(discoveryv1.EndpointSliceList)

	var result reconcile.Result
	var state resolver.State

	if cacheReady := r.runtimeManager.GetCache().WaitForCacheSync(ctx); !cacheReady {
		return result, fmt.Errorf("client cache failed to sync")
	}

	reconcileLog.V(2).Info("Listing EndpointSlices")
	err := k8sClient.List(ctx, endpointSliceList,
		client.MatchingLabels{
			discoveryv1.LabelServiceName: r.Service.Name,
		},
		client.InNamespace(r.Service.Namespace),
	)
	if err != nil {
		reconcileLog.Error(err, "Unable to list endpoint slices, will retry")
		result.Requeue = true
		return result, fmt.Errorf("error listing endpointslices: %w", err)
	}

	for _, slice := range endpointSliceList.Items {

		// shadow per-loop
		reconcileLog := reconcileLog.WithValues("endpointSlice", slice.Name)

		reconcileLog.V(2).Info("Processing EndpointSlice")

		if slice.AddressType == discoveryv1.AddressTypeFQDN {
			reconcileLog.V(1).Info("Skipping endpoint slice with non-IP type address", "endpointSlice", slice.Name)
			continue
		}

		reconcileLog.V(2).Info("Looking for matching ports")
		targetPortName := r.ServicePortName
		targetPortNumber := r.ServicePort
		if targetPortName == "" && targetPortNumber == 0 {
			reconcileLog.V(2).Info("No ports are set, defaulting port name")
			targetPortName = DefaultPortName
		}

		// use port number if no name is set but number is
		usePortName := true
		if targetPortName == "" && targetPortNumber > 0 {
			reconcileLog = reconcileLog.WithValues("servicePort", targetPortNumber)
			reconcileLog.V(2).Info("Using port number instead of name")
			usePortName = false
		} else {
			reconcileLog = reconcileLog.WithValues("servicePort", targetPortName)
		}

		sliceHasPort := false
		var actualPortNo int32 = 0
		for _, port := range slice.Ports {
			if usePortName && port.Name != nil && *port.Name == targetPortName {
				if port.Port == nil {
					reconcileLog.V(0).Info("Port with matching name has no port set, cannot set port")
					continue
				}
				reconcileLog.V(2).Info("Port found on slice")
				actualPortNo = *port.Port

				if actualPortNo == 0 {
					reconcileLog.V(1).Info("Endpoint had named port set to 0, selecting default")

					proto := corev1.ProtocolTCP
					if port.Protocol != nil {
						proto = *port.Protocol
						reconcileLog.V(2).Info("Port sets explicit protocol", "protocol", proto)
					}

					reconcileLog := reconcileLog.WithValues("protocol", proto)

					appProto := ""
					if port.AppProtocol != nil && *port.AppProtocol != "" {
						appProto = *port.AppProtocol
						reconcileLog.V(2).Info("Port sets explicit application protocol", "appProtocol", appProto)
					}
					if appProto == "" {
						reconcileLog.V(2).Info("Determining appProto from transport security")
						switch r.grpcSecurityProtocol {
						case "insecure":
							appProto = "http"
						case "tls":
							appProto = "https"
						}
					}
					reconcileLog = reconcileLog.WithValues("appProtocol", appProto)

					reconcileLog.V(2).Info("Looking up default port from system")
					portLookup, err := net.LookupPort(string(proto), appProto)
					if err != nil {
						reconcileLog.V(1).Error(err, "System port lookup failed with error, discarding port")
						continue
					}
					if portLookup == 0 {
						reconcileLog.V(1).Info("System port lookup returned port 0, discarding port")
						continue
					}
					actualPortNo = int32(portLookup)

				}

				sliceHasPort = true
				break
			}
		}
		if !sliceHasPort {
			reconcileLog.V(1).Info("Endpoint did not have matching port, discarding endpoint")
			continue
		}

		reconcileLog = reconcileLog.WithValues("port", actualPortNo)
		reconcileLog.V(2).Info("Endpoint has valid port, adding addresses")

		for endpointIndex, endpoint := range slice.Endpoints {
			reconcileLog := reconcileLog.WithValues("endpointIndex", endpointIndex)
			reconcileLog.V(2).Info("Processing endpoint")

			// treat unknown Ready (nil) as Ready
			if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
				reconcileLog.V(2).Info("Skipping undready endpoint")
				continue
			}

			if len(endpoint.Addresses) < 1 {
				reconcileLog.V(2).Info("Skipping endpoint with no addresses")
				continue
			}
			reconcileLog = reconcileLog.WithValues("IP", endpoint.Addresses[0])

			newAddr := resolver.Address{
				Addr: net.JoinHostPort(endpoint.Addresses[0], fmt.Sprintf("%d", actualPortNo)),
			}

			if endpoint.Hostname != nil && *endpoint.Hostname != "" {
				reconcileLog.V(2).Info("Endpoint has hostname, using as server name", "hostname", *endpoint.Hostname)
				newAddr.ServerName = *endpoint.Hostname
			}

			if hints := endpoint.Hints; hints != nil && len(r.preferredZoneWeights) > 0 {
				reconcileLog.V(2).Info("Endpoint has hints, checking for zone preference")
				maxWeight := 0
				for _, zone := range hints.ForZones {
					if weight, found := r.preferredZoneWeights[zone.Name]; found && weight > maxWeight {
						reconcileLog.V(2).Info("Found stronger preferred zone, adjusting weight", "zone", zone.Name, "weight", weight)
						maxWeight = weight
					}
				}
				if maxWeight > 0 {
					reconcileLog.V(2).Info("Set weighting for endpoint", "weight", maxWeight)
					newAddr = wrr.SetAddrInfo(newAddr, wrr.AddrInfo{Weight: uint32(maxWeight)})
				}
			}

			state.Addresses = append(state.Addresses, newAddr)
		}
	}

	reconcileLog.V(2).Info("Updating resolver state")
	err = r.clientConn.UpdateState(state)
	if err != nil {
		// errors from UpdateState can be ignored if the re-resolving won't change the outcome
		reconcileLog.Error(err, "Resolver state update errored. Will not retry")
	}

	reconcileLog.V(1).Info("Reconcile complete successfully", "addressCount", len(state.Addresses))
	return result, nil
}
