package kuberesolver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"google.golang.org/grpc/resolver"

	"github.com/go-logr/logr"
)

const DefaultScheme = "kube"

type KubeResolveBuilder struct {
	schemeName        string
	controllerManager manager.Manager
	runtimeContext    context.Context

	// config details
	defaultPortName  string
	defaultNamespace string
	managerClient    client.Client
	restConfig       *rest.Config
	zone             string
}

type KubeResolveBuilderOption func(*KubeResolveBuilder)

func NewKubeResolveBuilder(opts ...KubeResolveBuilderOption) (*KubeResolveBuilder, error) {

	var err error

	// default config
	builder := &KubeResolveBuilder{
		runtimeContext:  context.Background(),
		defaultPortName: DefaultPortName,
	}

	// apply options
	for _, opt := range opts {
		opt(builder)
	}

	// Create manager options
	managerOpts := manager.Options{}

	if builder.runtimeContext != nil {
		managerOpts.BaseContext = func() context.Context {
			return builder.runtimeContext
		}
	}
	if builder.managerClient != nil {
		managerOpts.NewClient = func(cache.Cache, *rest.Config, client.Options, ...client.Object) (client.Client, error) {
			return builder.managerClient, nil
		}
	}
	if builder.restConfig == nil {
		builder.restConfig, err = config.GetConfig()
		if err != nil {
			return nil, err
		}
	}

	// Setup a Manager
	builder.controllerManager, err = manager.New(builder.restConfig, managerOpts)
	if err != nil {
		return nil, err
	}

	if builder.runtimeContext == nil {
		builder.runtimeContext = signals.SetupSignalHandler()
	}

	// run the manager
	go builder.controllerManager.Start(builder.runtimeContext)

	return builder, nil
}

func (b *KubeResolveBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {

	var err error
	buildLog := b.controllerManager.GetLogger().WithName("Build").WithValues("target", target.URL.Redacted())

	ctx := b.runtimeContext

	res := &KubeResolver{
		grpcSecurityProtocol: opts.DialCreds.Info().SecurityProtocol,
		runtimeManager:       b.controllerManager,
		clientConn:           cc,
		defaultPortName:      b.defaultPortName,
		startSync:            make(chan interface{}),
		targetZone:           b.zone,
	}

	ctx, res.closeFunc = context.WithCancel(ctx)

	res.Service, res.ServicePortName, res.ServicePort, err = parseServiceNameAndNamespace(target.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("error parsing target host: %w", err)
	}
	if res.ServicePortName == "" && res.ServicePort == 0 {
		res.ServicePortName = b.defaultPortName
	}
	if res.Service.Namespace == "" {
		res.Service.Namespace = b.defaultNamespace
	}

	runtimeController, err := controller.NewUnmanaged("kuberesolver", b.controllerManager, controller.Options{
		Reconciler: res,
		LogConstructor: func(req *reconcile.Request) logr.Logger {
			log := b.controllerManager.GetLogger().WithName("kuberesolver").WithValues(
				"target", target.URL.Redacted(),
			)
			if req != nil {
				log = log.WithValues(
					"object", klog.KRef(req.Namespace, req.Name),
					"namespace", req.Namespace, "name", req.Name,
				)
			}
			return log
		},
	})
	if err != nil {
		buildLog.Error(err, "Error creating unmanaged controller")
		return nil, fmt.Errorf("error creating controller instance: %w", err)
	}

	// Watch EndpointSlices for updates
	err = runtimeController.Watch(&source.Kind{Type: &discoveryv1.EndpointSlice{}},
		&handler.EnqueueRequestForObject{},
		predicate.ResourceVersionChangedPredicate{},
		FilterForService(res.Service),
	)
	if err != nil {
		buildLog.Error(err, "Unable to watch endpoint slices")
		return nil, fmt.Errorf("error starting watch on endpoint slices: %w", err)
	}

	go func() {
		err := runtimeController.Start(ctx)
		log := runtimeController.GetLogger()
		if err != nil {
			log.Error(err, "Resolution controller halted with error")
		}
		log.V(0).Info("Resolution controller stopped")
	}()

	return res, nil
}

func (b *KubeResolveBuilder) Scheme() string {
	if b.schemeName == "" {
		return DefaultScheme
	}
	return b.schemeName
}

func WithContext(ctx context.Context) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.runtimeContext = ctx
	}
}

func WithClient(cli client.Client) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.managerClient = cli
	}
}

func WithDefaultNamespace(ns string) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.defaultNamespace = ns
	}
}

func WithDefaultPortName(name string) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.defaultPortName = name
	}
}

func WithRestConfig(c *rest.Config) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.restConfig = c
	}
}

func WithTopologyAwareRouting(forZone string) KubeResolveBuilderOption {
	return func(k *KubeResolveBuilder) {
		k.zone = forZone
	}
}

func FilterForService(nn types.NamespacedName) predicate.Predicate {
	return predicate.NewPredicateFuncs(func(o client.Object) bool {
		if o.GetNamespace() != nn.Namespace {
			return false
		}
		if o.GetLabels()[discoveryv1.LabelServiceName] != nn.Name {
			return false
		}
		return true
	})
}

// Parse service name & namespace as service:port
// port may be either a numeric port, a name, or omitted
// service may be in one of the following forms:
// - <name> (implied default namespace)
// - <name>.<namespace>
// - <name>.<namespace>.svc.cluster.local
// - <name>/<namespace>
func parseServiceNameAndNamespace(hostname string) (service types.NamespacedName, portName string, port uint16, err error) {
	var host string
	if strings.ContainsRune(hostname, ':') {
		host, portName, err = net.SplitHostPort(hostname)
		if err != nil {
			return service, portName, port, fmt.Errorf("error parsing URL host into host:port pair: %w", err)
		}
	} else {
		host = hostname
	}

	// port may be numberic or named
	if portNo, err := strconv.ParseUint(portName, 10, 16); err == nil {
		port = uint16(portNo)
		portName = ""
	}

	if strings.Contains(host, "/") {
		nameParts := strings.SplitN(host, "/", 2)
		service.Name = nameParts[0]
		service.Namespace = nameParts[1]
		return
	}

	host = strings.TrimSuffix(host, ".svc.cluster.local")
	domainParts := strings.SplitN(host, ".", 2)
	if len(domainParts) == 1 {
		service.Name = domainParts[0]
		return
	}
	if len(domainParts) > 1 {
		service.Name = domainParts[0]
		service.Namespace = domainParts[1]
		return
	}

	return service, portName, port, fmt.Errorf("unable to parse service name in any format: %s", hostname)
}
