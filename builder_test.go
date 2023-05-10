package kuberesolver

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func Test_parseServiceNameAndNamespace(t *testing.T) {
	tests := []struct {
		name         string
		hostname     string
		wantService  types.NamespacedName
		wantPortName string
		wantPort     uint16
		wantErr      bool
	}{
		{
			name:     "name and port",
			hostname: "mysvc:443",
			wantPort: 443,
			wantService: types.NamespacedName{
				Name: "mysvc",
			},
		},
		{
			name:         "name and port name",
			hostname:     "mysvc:grpc",
			wantPortName: "grpc",
			wantService: types.NamespacedName{
				Name: "mysvc",
			},
		},
		{
			name:     "name and no port",
			hostname: "mysvc",
			wantService: types.NamespacedName{
				Name: "mysvc",
			},
		},
		{
			name:     "name slash namespace",
			hostname: "mysvc/myns",
			wantService: types.NamespacedName{
				Name:      "mysvc",
				Namespace: "myns",
			},
		},
		{
			name:         "name slash namespace and port",
			hostname:     "mysvc/myns:grpc",
			wantPortName: "grpc",
			wantService: types.NamespacedName{
				Name:      "mysvc",
				Namespace: "myns",
			},
		},
		{
			name:     "name.namespace",
			hostname: "mysvc.myns",
			wantService: types.NamespacedName{
				Name:      "mysvc",
				Namespace: "myns",
			},
		},
		{
			name:     "name.namespace.svc.cluster.local",
			hostname: "mysvc.myns.svc.cluster.local",
			wantService: types.NamespacedName{
				Name:      "mysvc",
				Namespace: "myns",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotService, gotPortName, gotPort, err := parseServiceNameAndNamespace(tt.hostname)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseServiceNameAndNamespace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotService, tt.wantService) {
				t.Errorf("parseServiceNameAndNamespace() gotService = %v, want %v", gotService, tt.wantService)
			}
			if gotPortName != tt.wantPortName {
				t.Errorf("parseServiceNameAndNamespace() gotPortName = %v, want %v", gotPortName, tt.wantPortName)
			}
			if gotPort != tt.wantPort {
				t.Errorf("parseServiceNameAndNamespace() gotPort = %v, want %v", gotPort, tt.wantPort)
			}
		})
	}
}
