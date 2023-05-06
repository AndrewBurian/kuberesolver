package kuberesolver

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	discoveryv1 "k8s.io/api/discovery/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/go-logr/logr/testr"
)

type clientConnRecorder struct {
	resolver.ClientConn

	updates []*resolver.State
	err     error
}

func (c *clientConnRecorder) UpdateState(s resolver.State) error {
	c.updates = append(c.updates, &s)
	return nil
}

func (c *clientConnRecorder) ReportError(e error) {
	c.err = e
}

func (c *clientConnRecorder) state() (resolver.State, error) {
	return *c.updates[len(c.updates)-1], c.err
}

var (
	isReady    = true
	isNotReady = false
)

var (
	grpcPortName  = "grpc"
	otherPortName = "metrics"
)

var (
	grpcPortNo    = int32(8001)
	grpcPortNoAlt = int32(8002)
)

func TestInitialReconcile(t *testing.T) {

	verbosity := 1
	if testing.Verbose() {
		verbosity = 3
	}
	log.SetLogger(testr.NewWithOptions(t, testr.Options{
		Verbosity: verbosity,
	}))

	endpointSlices := []client.Object{
		&discoveryv1.EndpointSlice{
			ObjectMeta: v1.ObjectMeta{
				Name:      "slice-a",
				Namespace: "default",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "myservice",
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.1", "66.66.66.66"}},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Name: &grpcPortName,
					Port: &grpcPortNo,
				},
			},
		},

		&discoveryv1.EndpointSlice{
			ObjectMeta: v1.ObjectMeta{
				Name:      "slice-b",
				Namespace: "default",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: "myservice",
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{
				{Addresses: []string{"10.0.0.2", "66.66.66.66"}},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Name: &grpcPortName,
					Port: &grpcPortNoAlt,
				},
			},
		},
	}

	var expectedState = resolver.State{
		Addresses: []resolver.Address{{Addr: "10.0.0.1:8001"}, {Addr: "10.0.0.2:8002"}},
	}

	testenv := new(envtest.Environment)
	testenv.BinaryAssetsDirectory = "/Users/andrewburian/Library/Application Support/io.kubebuilder.envtest/k8s/1.27.1-darwin-arm64"
	cfg, err := testenv.Start()
	if err != nil {
		t.Fatalf("Error starting test environment: %s", err)
	}
	defer testenv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	kubeClient, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatal(err)
	}

	for i := range endpointSlices {
		if err = kubeClient.Create(ctx, endpointSlices[i], client.FieldOwner("test-runner")); err != nil {
			t.Fatalf("Error creating starting data: %s", err)
		}
	}

	resolveBuilder, err := NewKubeResolveBuilder(
		WithRestConfig(cfg),
		WithContext(ctx),
	)
	if err != nil {
		t.Fatalf("Error creating resolve builder: %s", err)
	}

	target := resolver.Target{
		URL: url.URL{
			Scheme: "kube",
			Host:   "myservice.mynamespace:grpc",
		},
	}

	conn := new(clientConnRecorder)

	// GRPC's process
	// syncronously create a resolution helper
	resolve, err := resolveBuilder.Build(target, conn, resolver.BuildOptions{
		DialCreds: insecure.NewCredentials(),
	})
	if err != nil {
		t.Fatalf("Failed to build resolver: %s", err)
	}

	// when calling GRPC.Dail, synchronously call ResolveNow
	resolve.ResolveNow(struct{}{})

	if conn.err != nil {
		t.Fatalf("Resolver reported error: %s", conn.err)
	}

	// let the dust settle
	time.Sleep(2 * time.Second)

	if len(conn.updates) == 0 {
		t.Fatal("Did not get updated state from resolver")
	}

	if err = conn.checkState(expectedState); err != nil {
		t.Fatal(err.Error())
	}

	// Updates to the API should automatically re-resolve
	newSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: v1.ObjectMeta{
			Name:      "slice-c",
			Namespace: "default",
			Labels: map[string]string{
				discoveryv1.LabelServiceName: "myservice",
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.3", "66.66.66.66"}},
		},
		Ports: []discoveryv1.EndpointPort{
			{
				Name: &grpcPortName,
				Port: &grpcPortNo,
			},
		},
	}
	if err = kubeClient.Create(ctx, newSlice); err != nil {
		t.Fatalf("Client create errored: %s", err)
	}

	expectedState.Addresses = append(expectedState.Addresses, resolver.Address{Addr: "10.0.0.3:8001"})

	time.Sleep(2 * time.Second)

	if err = conn.checkState(expectedState); err != nil {
		t.Fatal(err)
	}
}

func (c *clientConnRecorder) checkState(expect resolver.State) error {
	state, err := c.state()

	if err != nil {
		return err
	}

	if got, exp := len(state.Addresses), len(expect.Addresses); got != exp {
		return fmt.Errorf("Wrong number of addresses, expected %d got %d", exp, got)
	}

	for i := range state.Addresses {
		if got, exp := state.Addresses[i].Addr, expect.Addresses[i].Addr; exp != got {
			return fmt.Errorf("Wrong address at index %d, expected %s got %s", i, exp, got)
		}
	}

	return nil
}
