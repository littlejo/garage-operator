/*
Copyright 2026 Raj Singh.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"fmt"

	"github.com/rajsinghtech/garage-operator/internal/garageconfig"
)

// ValidateSupportedClusterReference rejects remote-kubeconfig references until
// the operator has a real remote Kubernetes client implementation. Controllers
// call this as well as admission so persisted objects cannot silently fall back
// to the local client when webhooks are unavailable.
func ValidateSupportedClusterReference(ref ClusterReference, field string) error {
	if ref.KubeConfigSecretRef != nil {
		return fmt.Errorf("%s.kubeConfigSecretRef is not supported; the operator can reference GarageClusters only through its configured Kubernetes client", field)
	}
	return nil
}

// ValidateClusterReference validates every cluster-reference field that is
// safe for the operator to resolve through its configured Kubernetes client.
func ValidateClusterReference(ref ClusterReference, field string) error {
	if err := ValidateSupportedClusterReference(ref, field); err != nil {
		return err
	}
	return validateNamespacedObjectReference(ref.Name, ref.Namespace, field)
}

func validateNamespacedObjectReference(name, namespace, field string) error {
	return garageconfig.ValidateNamespacedObjectReference(name, namespace, field)
}

func effectiveClusterReference(ref ClusterReference, objectNamespace string) (string, string) {
	namespace := ref.Namespace
	if namespace == "" {
		namespace = objectNamespace
	}
	return ref.Name, namespace
}

func clusterReferenceChanged(oldRef, newRef ClusterReference, objectNamespace string) bool {
	oldName, oldNamespace := effectiveClusterReference(oldRef, objectNamespace)
	newName, newNamespace := effectiveClusterReference(newRef, objectNamespace)
	return oldName != newName || oldNamespace != newNamespace
}

// BucketsShareGarageCluster reports whether two GarageBucket resources resolve
// to the same GarageCluster reference, so bucket IDs recorded on them compete
// for the same Garage instance.
func BucketsShareGarageCluster(a, b *GarageBucket) bool {
	if a == nil || b == nil {
		return false
	}
	aName, aNamespace := effectiveClusterReference(a.Spec.ClusterRef, a.Namespace)
	bName, bNamespace := effectiveClusterReference(b.Spec.ClusterRef, b.Namespace)
	return aName == bName && aNamespace == bNamespace
}

// FindBucketClaimConflict returns the first GarageBucket in items, other than
// self, that claims bucketID through spec.bucketId or status.bucketId and
// resolves to the same GarageCluster. Returns nil when the ID is unclaimed.
// A resource is matched on name and namespace because UIDs are not stable
// across fake clients in unit tests.
func FindBucketClaimConflict(self *GarageBucket, bucketID string, items []GarageBucket) *GarageBucket {
	if self == nil || bucketID == "" {
		return nil
	}
	for i := range items {
		other := &items[i]
		if other.Name == self.Name && other.Namespace == self.Namespace {
			continue
		}
		if !BucketsShareGarageCluster(other, self) {
			continue
		}
		if other.Spec.BucketID == bucketID || other.Status.BucketID == bucketID {
			return other
		}
	}
	return nil
}

// FindBucketStatusClaimConflict returns the first GarageBucket in items, other
// than self, whose recorded status.bucketId equals bucketID within the same
// GarageCluster. Two recorded claims on one bucket ID can only come from
// out-of-band edits, so callers must fail closed instead of picking a winner.
func FindBucketStatusClaimConflict(self *GarageBucket, bucketID string, items []GarageBucket) *GarageBucket {
	if self == nil || bucketID == "" {
		return nil
	}
	for i := range items {
		other := &items[i]
		if other.Name == self.Name && other.Namespace == self.Namespace {
			continue
		}
		if other.Status.BucketID != bucketID {
			continue
		}
		if !BucketsShareGarageCluster(other, self) {
			continue
		}
		return other
	}
	return nil
}
