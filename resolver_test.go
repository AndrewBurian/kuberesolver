package kuberesolver

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/resolver"

	discoveryv1 "k8s.io/api/discovery/v1"

	"github.com/go-logr/logr/testr"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func TestStateConstruction(t *testing.T) {

	var testcases = []struct {
		name        string
		cluster     discoveryv1.EndpointSliceList
		expectState resolver.State
	}{
		{
			name:        "empty state",
			cluster:     discoveryv1.EndpointSliceList{},
			expectState: resolver.State{},
		},
		{
			name: "simple state",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
				},
			},
		},
		{
			name: "extra addresses",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1", "9.9.9.9", "ignored addr"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
				},
			},
		},
		{
			name: "extra ports",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &otherPortName,
								Port: &grpcPortNoAlt,
							},
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
				},
			},
		},
		{
			name: "multiple endpoints",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
							{
								Addresses: []string{"10.0.0.2"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8001",
					},
				},
			},
		},
		{
			name: "topology aware endpoints",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								},
							},
							{
								Addresses: []string{"10.0.0.2"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-other"}},
								},
							},
							{
								Addresses: []string{"10.0.0.3"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.3:8001",
					},
				},
			},
		},
		{
			name: "mixed topology aware endpoints",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								},
							},
							{
								Addresses: []string{"10.0.0.2"},
							},
							{
								Addresses: []string{"10.0.0.3"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8001",
					},
					{
						Addr: "10.0.0.3:8001",
					},
				},
			},
		},
		{
			name: "no topology aligned endpoints",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-other"}},
								},
							},
							{
								Addresses: []string{"10.0.0.2"},
								Hints: &discoveryv1.EndpointHints{
									ForZones: []discoveryv1.ForZone{{Name: "zone-other2"}},
								},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8001",
					},
				},
			},
		},
		{
			name: "unready endpoints",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
								Conditions: discoveryv1.EndpointConditions{
									Ready: &isReady,
								},
							},
							{
								Addresses: []string{"9.9.9.9"},
								Conditions: discoveryv1.EndpointConditions{
									Ready: &isNotReady,
								},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
				},
			},
		},
		{
			name: "multiple slices",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.2"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8001",
					},
				},
			},
		},
		{
			name: "multiple endpoint slice",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
							{
								Addresses: []string{"10.0.0.2"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8001",
					},
				},
			},
		},
		{
			name: "multiple slices with different ports",
			cluster: discoveryv1.EndpointSliceList{
				Items: []discoveryv1.EndpointSlice{
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.1"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNo,
							},
						},
					},
					{
						AddressType: discoveryv1.AddressTypeIPv4,
						Endpoints: []discoveryv1.Endpoint{
							{
								Addresses: []string{"10.0.0.2"},
							},
						},
						Ports: []discoveryv1.EndpointPort{
							{
								Name: &grpcPortName,
								Port: &grpcPortNoAlt,
							},
						},
					},
				},
			},
			expectState: resolver.State{
				Addresses: []resolver.Address{
					{
						Addr: "10.0.0.1:8001",
					},
					{
						Addr: "10.0.0.2:8002",
					},
				},
			},
		},
	}

	r := new(KubeResolver)
	r.targetZone = "zone-a"
	r.defaultPortName = grpcPortName

	verbosity := 0
	if testing.Verbose() {
		verbosity = 3
	}
	log.SetLogger(testr.NewWithOptions(t, testr.Options{
		Verbosity: verbosity,
	}))

	for i := range testcases {
		test := testcases[i]
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			state := r.createState(&test.cluster, log.Log.WithName(t.Name()))
			if err := compareStates(state, test.expectState); err != nil {
				t.Error(err)
			}
		})

	}
}

func compareStates(state, expect resolver.State) error {

	// Check all addresses
	if got, exp := len(state.Addresses), len(expect.Addresses); got != exp {
		return fmt.Errorf("Wrong number of addresses, expected %d got %d", exp, got)
	}

	expectAddrToStateIndex := make(map[string]int)
	for i := range expect.Addresses {
		expectAddrToStateIndex[expect.Addresses[i].Addr] = -1
	}
	for i := range state.Addresses {
		index, found := expectAddrToStateIndex[state.Addresses[i].Addr]
		if !found {
			return fmt.Errorf("Unexpected address in state: %s", state.Addresses[i].Addr)
		}
		if index >= 0 {
			return fmt.Errorf("Duplicate address in state: %s", state.Addresses[i].Addr)
		}

		expectAddrToStateIndex[state.Addresses[i].Addr] = i
	}
	for addr, index := range expectAddrToStateIndex {
		if index == -1 {
			return fmt.Errorf("Missing address in state: %s", addr)
		}
	}

	// check balance attrs
	for i := range expect.Addresses {
		stateIndex := expectAddrToStateIndex[expect.Addresses[i].Addr]
		if !expect.Addresses[i].BalancerAttributes.Equal(state.Addresses[stateIndex].BalancerAttributes) {
			return fmt.Errorf("Balance attributes for %s aren't equal", state.Addresses[i].Addr)
		}
	}

	// Check attributes
	if !expect.Attributes.Equal(state.Attributes) {
		return fmt.Errorf("State attributes aren't equal")
	}

	return nil
}
