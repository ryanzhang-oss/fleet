/*
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
*/

package keys

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	fleetv1alpha1 "go.goms.io/fleet/apis/v1alpha1"
)

// ClusterWideKey is the object key which is a unique identifier under a cluster, across all resources.
type ClusterWideKey struct {
	fleetv1alpha1.ResourceIdentifier
}

// String returns the key's printable info with format:
// "<GroupVersion>, kind=<Kind>, <NamespaceKey>"
func (k ClusterWideKey) String() string {
	return fmt.Sprintf("%s, kind=%s, %s", k.GroupVersion().String(), k.Kind, k.NamespaceKey())
}

// NamespaceKey returns the traditional key of an object.
func (k *ClusterWideKey) NamespaceKey() string {
	if len(k.Namespace) > 0 {
		return k.Namespace + "/" + k.Name
	}

	return k.Name
}

// GroupVersionKind returns the group, version, and kind of resource being referenced.
func (k *ClusterWideKey) GroupVersionKind() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   k.Group,
		Version: k.Version,
		Kind:    k.Kind,
	}
}

// GroupVersion returns the group and version of resource being referenced.
func (k *ClusterWideKey) GroupVersion() schema.GroupVersion {
	return schema.GroupVersion{
		Group:   k.Group,
		Version: k.Version,
	}
}

// GroupKind returns the group and kind of resource being referenced.
func (k *ClusterWideKey) GroupKind() schema.GroupKind {
	return schema.GroupKind{
		Group: k.Group,
		Kind:  k.Kind,
	}
}

// getClusterWideKeyForObject generates a ClusterWideKey for object.
func GetClusterWideKeyForObject(obj interface{}) (ClusterWideKey, error) {
	key := ClusterWideKey{}

	runtimeObject, ok := obj.(runtime.Object)
	if !ok {
		klog.Errorf("Invalid object")
		return key, fmt.Errorf("not runtime object")
	}

	metaInfo, err := meta.Accessor(obj)
	if err != nil { // should not happen
		return key, fmt.Errorf("object has no meta: %w", err)
	}

	gvk := runtimeObject.GetObjectKind().GroupVersionKind()
	key.Group = gvk.Group
	key.Version = gvk.Version
	key.Kind = gvk.Kind
	key.Namespace = metaInfo.GetNamespace()
	key.Name = metaInfo.GetName()

	return key, nil
}
