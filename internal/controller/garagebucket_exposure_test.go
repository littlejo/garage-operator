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

package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	garagev1beta1 "github.com/rajsinghtech/garage-operator/api/v1beta1"
	garagev1beta2 "github.com/rajsinghtech/garage-operator/api/v1beta2"
)

func websiteExposureTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		policyv1.AddToScheme,
		networkingv1.AddToScheme,
		garagev1beta1.AddToScheme,
		garagev1beta2.AddToScheme,
		gatewayv1.Install,
	} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

// websiteExposureTestClient builds a fake client whose REST mapper reports
// HTTPRoute as available (or not, when gatewayAPI is false). It returns the
// client and the scheme it was built with (the fake client does not expose
// its scheme through the client.Client interface).
func websiteExposureTestClient(t *testing.T, gatewayAPI bool, objects ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := websiteExposureTestScheme(t)
	// DefaultRESTMapper resolves a version-less RESTMapping against its
	// default group versions, so the groups being probed must be listed.
	defaultGroupVersions := []schema.GroupVersion{networkingv1.SchemeGroupVersion}
	if gatewayAPI {
		defaultGroupVersions = append(defaultGroupVersions, gatewayv1.SchemeGroupVersion)
	}
	mapper := meta.NewDefaultRESTMapper(defaultGroupVersions)
	mapper.Add(networkingv1.SchemeGroupVersion.WithKind("Ingress"), meta.RESTScopeNamespace)
	if gatewayAPI {
		mapper.Add(gatewayv1.SchemeGroupVersion.WithKind("HTTPRoute"), meta.RESTScopeNamespace)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRESTMapper(mapper).
		WithStatusSubresource(&garagev1beta1.GarageBucket{}).
		WithObjects(objects...).
		Build()
	return c, scheme
}

func websiteExposureTestBucket(namespace string) *garagev1beta1.GarageBucket {
	return &garagev1beta1.GarageBucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "site",
			Namespace:  namespace,
			UID:        types.UID("bucket-uid-1"),
			Generation: 1,
		},
		Spec: garagev1beta1.GarageBucketSpec{
			ClusterRef: garagev1beta1.ClusterReference{Name: "garage"},
		},
		Status: garagev1beta1.GarageBucketStatus{
			BucketID:    "bucket-id-1",
			GlobalAlias: "site",
		},
	}
}

func websiteExposureTestCluster() *garagev1beta2.GarageCluster {
	return &garagev1beta2.GarageCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "garage",
			Namespace:  "garage-ns",
			UID:        types.UID("cluster-uid-1"),
			Generation: 1,
		},
		Spec: garagev1beta2.GarageClusterSpec{
			WebAPI: &garagev1beta2.WebAPIConfig{
				RootDomain: ".example.com",
			},
		},
	}
}

func websiteExposureCondition(t *testing.T, bucket *garagev1beta1.GarageBucket) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(bucket.Status.Conditions, garagev1beta1.ConditionWebsiteExposed)
	if c == nil {
		t.Fatalf("WebsiteExposed condition not found in %+v", bucket.Status.Conditions)
	}
	return c
}

func TestWebsiteExposureIngressDerivedHost(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{
			IngressClassName: "traefik",
		},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	result, err := r.reconcileWebsiteExposure(ctx, bucket, cluster)
	if err != nil {
		t.Fatalf("reconcileWebsiteExposure: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue after successful exposure, got %v", result.RequeueAfter)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, ingress); err != nil {
		t.Fatalf("expected Ingress in the cluster namespace: %v", err)
	}
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "traefik" {
		t.Fatalf("ingressClassName = %v, want traefik", ingress.Spec.IngressClassName)
	}
	if len(ingress.Spec.Rules) != 1 || ingress.Spec.Rules[0].Host != "site.example.com" {
		t.Fatalf("rules = %+v, want single rule for site.example.com", ingress.Spec.Rules)
	}
	backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if backend.Name != "garage" || backend.Port.Name != "web" {
		t.Fatalf("backend = %+v, want service garage port web", backend)
	}
	if !metav1.IsControlledBy(ingress, bucket) {
		t.Fatalf("ingress is not controlled by the bucket: %+v", ingress.OwnerReferences)
	}

	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionTrue || cond.Reason != "Exposed" {
		t.Fatalf("condition = %+v, want True/Exposed", cond)
	}
	if bucket.Status.WebsiteExposure == nil || !bucket.Status.WebsiteExposure.Ready {
		t.Fatalf("status.websiteExposure = %+v, want ready", bucket.Status.WebsiteExposure)
	}
	if bucket.Status.WebsiteExposure.Namespace != cluster.Namespace {
		t.Fatalf("status namespace = %q, want %q", bucket.Status.WebsiteExposure.Namespace, cluster.Namespace)
	}
}

func TestWebsiteExposureIngressExplicitHostAndTLS(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Host:          "site.example.com",
		TLSSecretName: "site-tls",
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{
			Annotations: map[string]string{"cert-manager.io/cluster-issuer": "letsencrypt"},
			Labels:      map[string]string{"team": "web"},
		},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatalf("reconcileWebsiteExposure: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, ingress); err != nil {
		t.Fatal(err)
	}
	if len(ingress.Spec.TLS) != 1 || ingress.Spec.TLS[0].SecretName != "site-tls" ||
		len(ingress.Spec.TLS[0].Hosts) != 1 || ingress.Spec.TLS[0].Hosts[0] != "site.example.com" {
		t.Fatalf("tls = %+v", ingress.Spec.TLS)
	}
	if ingress.Annotations["cert-manager.io/cluster-issuer"] != "letsencrypt" {
		t.Fatalf("annotations = %+v", ingress.Annotations)
	}
	if ingress.Labels["team"] != "web" || ingress.Labels[labelBucketRef] != "site" {
		t.Fatalf("labels = %+v", ingress.Labels)
	}
}

func TestWebsiteExposureIngressHostMismatch(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Host:    "other.example.com",
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatalf("reconcileWebsiteExposure should surface the mismatch on the condition, not error: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, ingress)
	if err != nil && !k8errors.IsNotFound(err) {
		t.Fatalf("expected no Ingress for a mismatching host: %v", err)
	}
	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionFalse || cond.Reason != garagev1beta1.ReasonReconcileFailed {
		t.Fatalf("condition = %+v, want False/ReconcileFailed", cond)
	}
	if bucket.Status.WebsiteExposure == nil || bucket.Status.WebsiteExposure.Ready {
		t.Fatalf("status = %+v, want not ready", bucket.Status.WebsiteExposure)
	}
}

func TestWebsiteExposureWaitingForAlias(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	bucket.Status.GlobalAlias = ""
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	result, err := r.reconcileWebsiteExposure(ctx, bucket, cluster)
	if err != nil {
		t.Fatalf("reconcileWebsiteExposure: %v", err)
	}
	if result.RequeueAfter != RequeueAfterShort {
		t.Fatalf("requeue = %v, want %v while waiting for the alias", result.RequeueAfter, RequeueAfterShort)
	}
	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionFalse || cond.Reason != "WaitingForAlias" {
		t.Fatalf("condition = %+v, want False/WaitingForAlias", cond)
	}
}

func TestWebsiteExposureIngressDeletedWhenSpecRemoved(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatal(err)
	}

	// Re-fetch from the store: the previous call mutated the in-memory object.
	stored := &garagev1beta1.GarageBucket{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: bucket.Namespace, Name: bucket.Name}, stored); err != nil {
		t.Fatal(err)
	}
	stored.Spec.WebsiteExposure = nil
	if err := c.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}

	if _, err := r.reconcileWebsiteExposure(ctx, stored, cluster); err != nil {
		t.Fatal(err)
	}
	ingress := &networkingv1.Ingress{}
	err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, ingress)
	if err != nil && !k8errors.IsNotFound(err) {
		t.Fatalf("expected the Ingress to be deleted: %v", err)
	}
	if stored.Status.WebsiteExposure != nil {
		t.Fatalf("status.websiteExposure = %+v, want cleared", stored.Status.WebsiteExposure)
	}
	if meta.FindStatusCondition(stored.Status.Conditions, garagev1beta1.ConditionWebsiteExposed) != nil {
		t.Fatalf("WebsiteExposed condition should be removed: %+v", stored.Status.Conditions)
	}
}

func TestWebsiteExposureForeignObjectRefused(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	foreign := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{Name: "site-website", Namespace: cluster.Namespace},
	}
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster, foreign)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatalf("the refusal must surface on the condition, not as a reconcile error: %v", err)
	}
	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionFalse || cond.Reason != garagev1beta1.ReasonReconcileFailed {
		t.Fatalf("condition = %+v, want False/ReconcileFailed", cond)
	}
	if !strings.Contains(cond.Message, "not controlled by") {
		t.Fatalf("condition message = %q, want ownership refusal", cond.Message)
	}
	// The foreign object must be untouched: no owner reference added.
	if err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, foreign); err != nil {
		t.Fatal(err)
	}
	if len(foreign.OwnerReferences) != 0 {
		t.Fatalf("foreign Ingress was mutated: %+v", foreign.OwnerReferences)
	}
}

func TestWebsiteExposureHTTPRouteCreated(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Host: "site.example.com",
		Gateway: &garagev1beta1.WebsiteExposureGatewayConfig{
			ParentRefs: []garagev1beta1.ParentReferenceConfig{
				{Name: "public-gateway", SectionName: "http"},
			},
		},
	}
	c, scheme := websiteExposureTestClient(t, true, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatalf("reconcileWebsiteExposure: %v", err)
	}

	route := &gatewayv1.HTTPRoute{}
	if err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, route); err != nil {
		t.Fatalf("expected HTTPRoute in the cluster namespace: %v", err)
	}
	if len(route.Spec.Hostnames) != 1 || route.Spec.Hostnames[0] != "site.example.com" {
		t.Fatalf("hostnames = %v", route.Spec.Hostnames)
	}
	if len(route.Spec.ParentRefs) != 1 || route.Spec.ParentRefs[0].Name != "public-gateway" {
		t.Fatalf("parentRefs = %+v", route.Spec.ParentRefs)
	}
	if route.Spec.ParentRefs[0].SectionName == nil || *route.Spec.ParentRefs[0].SectionName != "http" {
		t.Fatalf("sectionName = %v", route.Spec.ParentRefs[0].SectionName)
	}
	rule := route.Spec.Rules[0]
	if rule.Matches[0].Path == nil || *rule.Matches[0].Path.Type != gatewayv1.PathMatchPathPrefix ||
		rule.Matches[0].Path.Value == nil || *rule.Matches[0].Path.Value != "/" {
		t.Fatalf("matches = %+v", rule.Matches)
	}
	backend := rule.BackendRefs[0].BackendRef
	if backend.Name != "garage" {
		t.Fatalf("backend name = %q", backend.Name)
	}
	if backend.Namespace != nil {
		t.Fatalf("same-namespace backend must not set an explicit namespace: %v", *backend.Namespace)
	}
	if backend.Port == nil || *backend.Port != gatewayv1.PortNumber(3902) {
		t.Fatalf("backend port = %v, want 3902", backend.Port)
	}
	if !metav1.IsControlledBy(route, bucket) {
		t.Fatalf("route is not controlled by the bucket: %+v", route.OwnerReferences)
	}
	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionTrue || cond.Reason != "Exposed" {
		t.Fatalf("condition = %+v, want True/Exposed", cond)
	}
	if bucket.Status.WebsiteExposure.Type != "HTTPRoute" {
		t.Fatalf("status type = %q", bucket.Status.WebsiteExposure.Type)
	}
}

func TestWebsiteExposureHTTPRouteCRDsUnavailable(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("garage-ns")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Host: "site.example.com",
		Gateway: &garagev1beta1.WebsiteExposureGatewayConfig{
			ParentRefs: []garagev1beta1.ParentReferenceConfig{{Name: "public-gateway"}},
		},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	result, err := r.reconcileWebsiteExposure(ctx, bucket, cluster)
	if err != nil {
		t.Fatalf("missing CRDs must not fail the bucket: %v", err)
	}
	if result.RequeueAfter != RequeueAfterDrift {
		t.Fatalf("requeue = %v, want the drift interval", result.RequeueAfter)
	}
	cond := websiteExposureCondition(t, bucket)
	if cond.Status != metav1.ConditionFalse || cond.Reason != "GatewayAPIUnavailable" {
		t.Fatalf("condition = %+v, want False/GatewayAPIUnavailable", cond)
	}
}

func TestWebsiteExposureCrossNamespace(t *testing.T) {
	ctx := context.Background()
	bucket := websiteExposureTestBucket("apps")
	cluster := websiteExposureTestCluster()
	bucket.Spec.WebsiteExposure = &garagev1beta1.WebsiteExposureConfig{
		Ingress: &garagev1beta1.WebsiteExposureIngressConfig{},
	}
	c, scheme := websiteExposureTestClient(t, false, bucket, cluster)
	r := &GarageBucketReconciler{Client: c, Scheme: scheme}

	if _, err := r.reconcileWebsiteExposure(ctx, bucket, cluster); err != nil {
		t.Fatalf("reconcileWebsiteExposure: %v", err)
	}

	ingress := &networkingv1.Ingress{}
	if err := c.Get(ctx, types.NamespacedName{Name: "site-website", Namespace: cluster.Namespace}, ingress); err != nil {
		t.Fatalf("expected the Ingress in the cluster namespace: %v", err)
	}
	if metav1.IsControlledBy(ingress, bucket) {
		t.Fatalf("cross-namespace resource must not carry a bucket owner reference: %+v", ingress.OwnerReferences)
	}
	if ingress.Spec.Rules[0].Host != "site.example.com" {
		t.Fatalf("host = %q", ingress.Spec.Rules[0].Host)
	}
}
