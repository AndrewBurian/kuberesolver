# kuberesolver

GRPC name resolver plugin for Kubernetes.


## Basic Usage

Instantiate a builder instance with any relevant config into your GRPC client.

```go
// import google.golang.org/grpc/resolver

builder := kuberesolver.NewKubeResolveBuilder(
    kuberesolver.WithDefaultPortName("grpc"),
    kuberesolver.WithTopologyAwareRouting("us-east1-a"),
)

resolver.Register(builder)
```

Create headless Kubernetes services for your targets. The resolver won't use the clusterIP, so headless is best, but there is no harm in using a service that sets one as it will be ignored.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-service
  namespace: my-ns
  annotations:
    service.kubernetes.io/topology-mode: Auto
    # Enable topology aware routing with Auto, or disable explicitly with 'off'
    # Causes kubernetes to allocate endpoints to zones close to them for less
    # cross-zone traffic. The balancing and assignment logic is done by k8s, not kuberesolver.
    # https://kubernetes.io/docs/concepts/services-networking/topology-aware-routing/
spec:
  selector:
    app.kubernetes.io/name: MyGrpcApp
  clusterIP: None
  ports:
    - name: grpc
      protocol: TCP
      targetPort: rpc-port
      # reference a valid spec.containers[].ports[].name from your deployemnt
      # in targetPort
```

Target the service in your GRPC client

```go
// target name is kube://<service-name>.<service-namespace>[.svc.cluster.local][:<service-port-name>]
grpc.Dial("kube://my-service.my-ns:grpc")

// other versions
grpc.Dial("kube://my-service.my-ns") // default port name from builder config
grpc.Dial("kube://my-service") // default namespace from builder config
grpc.Dial("kube://my-service.my-ns.svc.cluster.local") // full service path so DNS fallback works if kuberesolver is removed

grpc.Dial("myscheme://my-service") // if builder uses `WithScheme("myscheme")`
```