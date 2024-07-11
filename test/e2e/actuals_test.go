/*
Copyright (c) Microsoft Corporation.
Licensed under the MIT license.
*/

package e2e

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	. "github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	placementv1beta1 "go.goms.io/fleet/apis/placement/v1beta1"
	"go.goms.io/fleet/pkg/controllers/clusterresourceplacement"
	"go.goms.io/fleet/pkg/controllers/work"
	scheduler "go.goms.io/fleet/pkg/scheduler/framework"
	"go.goms.io/fleet/pkg/utils/condition"
	"go.goms.io/fleet/test/e2e/framework"
)

func validateWorkNamespaceOnCluster(cluster *framework.Cluster, name types.NamespacedName) error {
	ns := &corev1.Namespace{}
	if err := cluster.KubeClient.Get(ctx, name, ns); err != nil {
		return err
	}

	// Use the object created in the hub cluster as reference; this helps to avoid the trouble
	// of having to ignore default fields in the spec.
	wantNS := &corev1.Namespace{}
	if err := hubClient.Get(ctx, name, wantNS); err != nil {
		return err
	}

	if diff := cmp.Diff(
		ns, wantNS,
		ignoreNamespaceStatusField,
		ignoreObjectMetaAutoGeneratedFields,
		ignoreObjectMetaAnnotationField,
	); diff != "" {
		return fmt.Errorf("work namespace diff (-got, +want): %s", diff)
	}
	return nil
}

func validateAnnotationOfWorkNamespaceOnCluster(cluster *framework.Cluster, wantAnnotations map[string]string) error {
	workNamespaceName := fmt.Sprintf(workNamespaceNameTemplate, GinkgoParallelProcess())
	ns := &corev1.Namespace{}
	if err := cluster.KubeClient.Get(ctx, types.NamespacedName{Name: workNamespaceName}, ns); err != nil {
		return err
	}

	for k, v := range wantAnnotations {
		if ns.Annotations[k] != v {
			return fmt.Errorf("work namespace annotation %s got %s, want %s", k, ns.Annotations[k], v)
		}
	}
	return nil
}

func validateConfigMapOnCluster(cluster *framework.Cluster, name types.NamespacedName) error {
	configMap := &corev1.ConfigMap{}
	if err := cluster.KubeClient.Get(ctx, name, configMap); err != nil {
		return err
	}

	// Use the object created in the hub cluster as reference.
	wantConfigMap := &corev1.ConfigMap{}
	if err := hubClient.Get(ctx, name, wantConfigMap); err != nil {
		return err
	}

	if diff := cmp.Diff(
		configMap, wantConfigMap,
		ignoreObjectMetaAutoGeneratedFields,
		ignoreObjectMetaAnnotationField,
	); diff != "" {
		return fmt.Errorf("app config map diff (-got, +want): %s", diff)
	}

	return nil
}

func validateOverrideAnnotationOfConfigMapOnCluster(cluster *framework.Cluster, wantAnnotations map[string]string) error {
	workNamespaceName := fmt.Sprintf(workNamespaceNameTemplate, GinkgoParallelProcess())
	appConfigMapName := fmt.Sprintf(appConfigMapNameTemplate, GinkgoParallelProcess())

	configMap := &corev1.ConfigMap{}
	if err := cluster.KubeClient.Get(ctx, types.NamespacedName{Namespace: workNamespaceName, Name: appConfigMapName}, configMap); err != nil {
		return err
	}

	for k, v := range wantAnnotations {
		if configMap.Annotations[k] != v {
			return fmt.Errorf("app config map annotation %s got %s, want %s", k, configMap.Annotations[k], v)
		}
	}
	return nil
}

func workNamespaceAndConfigMapPlacedOnClusterActual(cluster *framework.Cluster) func() error {
	workNamespaceName := fmt.Sprintf(workNamespaceNameTemplate, GinkgoParallelProcess())
	appConfigMapName := fmt.Sprintf(appConfigMapNameTemplate, GinkgoParallelProcess())

	return func() error {
		if err := validateWorkNamespaceOnCluster(cluster, types.NamespacedName{Name: workNamespaceName}); err != nil {
			return err
		}

		return validateConfigMapOnCluster(cluster, types.NamespacedName{Namespace: workNamespaceName, Name: appConfigMapName})
	}
}

func workNamespacePlacedOnClusterActual(cluster *framework.Cluster) func() error {
	workNamespaceName := fmt.Sprintf(workNamespaceNameTemplate, GinkgoParallelProcess())

	return func() error {
		return validateWorkNamespaceOnCluster(cluster, types.NamespacedName{Name: workNamespaceName})
	}
}

func crpScheduleFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             scheduler.NotFullyScheduledReason,
		},
	}
}

func crpSchedulePartiallyFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             scheduler.NotFullyScheduledReason,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.OverrideNotSpecifiedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.WorkSynchronizedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAppliedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ApplySucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAvailableConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.AvailableReason,
			ObservedGeneration: generation,
		},
	}
}

func crpRolloutStuckConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.RolloutNotStartedYetReason,
			ObservedGeneration: generation,
		},
	}
}

func crpAppliedFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.OverrideNotSpecifiedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.WorkSynchronizedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAppliedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.ApplyFailedReason,
			ObservedGeneration: generation,
		},
	}
}

func crpNotAvailableConditions(generation int64, hasOverride bool) []metav1.Condition {
	overrideConditionReason := condition.OverrideNotSpecifiedReason
	if hasOverride {
		overrideConditionReason = condition.OverriddenSucceededReason
	}
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             overrideConditionReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.WorkSynchronizedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAppliedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ApplySucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAvailableConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.NotAvailableYetReason,
			ObservedGeneration: generation,
		},
	}
}

func crpRolloutCompletedConditions(generation int64, hasOverride bool) []metav1.Condition {
	overrideConditionReason := condition.OverrideNotSpecifiedReason
	if hasOverride {
		overrideConditionReason = condition.OverriddenSucceededReason
	}
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             overrideConditionReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.WorkSynchronizedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAppliedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ApplySucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementAvailableConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.AvailableReason,
			ObservedGeneration: generation,
		},
	}
}

func resourcePlacementSyncPendingConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ScheduleSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.RolloutNotStartedYetReason,
			ObservedGeneration: generation,
		},
	}
}

func resourcePlacementApplyFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ScheduleSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.OverrideNotSpecifiedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.AllWorkSyncedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourcesAppliedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.WorkNotAppliedReason,
			ObservedGeneration: generation,
		},
	}
}

func resourcePlacementRolloutCompletedConditions(generation int64, resourceIsTrackable bool, hasOverride bool) []metav1.Condition {
	availableConditionReason := work.WorkNotTrackableReason
	if resourceIsTrackable {
		availableConditionReason = condition.AllWorkAvailableReason
	}
	overrideConditionReason := condition.OverrideNotSpecifiedReason
	if hasOverride {
		overrideConditionReason = condition.OverriddenSucceededReason
	}

	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ScheduleSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             overrideConditionReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceWorkSynchronizedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.AllWorkSyncedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourcesAppliedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.AllWorkAppliedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourcesAvailableConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             availableConditionReason,
			ObservedGeneration: generation,
		},
	}
}

func resourcePlacementRolloutFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             clusterresourceplacement.ResourceScheduleFailedReason,
		},
	}
}

func crpOverrideFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.OverriddenFailedReason,
			ObservedGeneration: generation,
		},
	}
}

func resourcePlacementOverrideFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ScheduleSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceOverriddenConditionType),
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			Reason:             condition.OverriddenFailedReason,
		},
	}
}

func resourcePlacementWorkSynchronizedFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ResourceScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.ScheduleSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ResourceOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             condition.OverriddenSucceededReason,
		},
		{
			Type:               string(placementv1beta1.ResourceWorkSynchronizedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.SyncWorkFailedReason,
			ObservedGeneration: generation,
		},
	}
}

func crpWorkSynchronizedFailedConditions(generation int64) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             scheduler.FullyScheduledReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.RolloutStartedReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementOverriddenConditionType),
			Status:             metav1.ConditionTrue,
			Reason:             condition.OverriddenSucceededReason,
			ObservedGeneration: generation,
		},
		{
			Type:               string(placementv1beta1.ClusterResourcePlacementWorkSynchronizedConditionType),
			Status:             metav1.ConditionFalse,
			Reason:             condition.WorkNotSynchronizedYetReason,
			ObservedGeneration: generation,
		},
	}
}

func workResourceIdentifiers() []placementv1beta1.ResourceIdentifier {
	workNamespaceName := fmt.Sprintf(workNamespaceNameTemplate, GinkgoParallelProcess())
	appConfigMapName := fmt.Sprintf(appConfigMapNameTemplate, GinkgoParallelProcess())

	return []placementv1beta1.ResourceIdentifier{
		{
			Kind:    "Namespace",
			Name:    workNamespaceName,
			Version: "v1",
		},
		{
			Kind:      "ConfigMap",
			Name:      appConfigMapName,
			Version:   "v1",
			Namespace: workNamespaceName,
		},
	}
}

func crpStatusWithOverrideUpdatedActual(
	wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier,
	wantSelectedClusters []string,
	wantObservedResourceIndex string,
	wantClusterResourceOverrides []string,
	wantResourceOverrides []placementv1beta1.NamespacedName) func() error {
	crpName := fmt.Sprintf(crpNameTemplate, GinkgoParallelProcess())

	return func() error {
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			return err
		}

		var wantPlacementStatus []placementv1beta1.ResourcePlacementStatus
		for _, name := range wantSelectedClusters {
			wantPlacementStatus = append(wantPlacementStatus, placementv1beta1.ResourcePlacementStatus{
				ClusterName:                        name,
				Conditions:                         resourcePlacementRolloutCompletedConditions(crp.Generation, true, true),
				ApplicableResourceOverrides:        wantResourceOverrides,
				ApplicableClusterResourceOverrides: wantClusterResourceOverrides,
			})
		}

		wantStatus := placementv1beta1.ClusterResourcePlacementStatus{
			Conditions:            crpRolloutCompletedConditions(crp.Generation, true),
			PlacementStatuses:     wantPlacementStatus,
			SelectedResources:     wantSelectedResourceIdentifiers,
			ObservedResourceIndex: wantObservedResourceIndex,
		}
		if diff := cmp.Diff(crp.Status, wantStatus, crpStatusCmpOptions...); diff != "" {
			return fmt.Errorf("CRP status diff (-got, +want): %s", diff)
		}
		return nil
	}
}

func crpStatusUpdatedActual(wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier, wantSelectedClusters, wantUnselectedClusters []string, wantObservedResourceIndex string) func() error {
	crpName := fmt.Sprintf(crpNameTemplate, GinkgoParallelProcess())
	return customizedCRPStatusUpdatedActual(crpName, wantSelectedResourceIdentifiers, wantSelectedClusters, wantUnselectedClusters, wantObservedResourceIndex, true)
}

func crpStatusWithOverrideUpdatedFailedActual(
	wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier,
	wantSelectedClusters []string,
	wantObservedResourceIndex string,
	wantClusterResourceOverrides []string,
	wantResourceOverrides []placementv1beta1.NamespacedName) func() error {
	crpName := fmt.Sprintf(crpNameTemplate, GinkgoParallelProcess())

	return func() error {
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			return err
		}

		var wantPlacementStatus []placementv1beta1.ResourcePlacementStatus
		for _, name := range wantSelectedClusters {
			wantPlacementStatus = append(wantPlacementStatus, placementv1beta1.ResourcePlacementStatus{
				ClusterName:                        name,
				Conditions:                         resourcePlacementOverrideFailedConditions(crp.Generation),
				ApplicableResourceOverrides:        wantResourceOverrides,
				ApplicableClusterResourceOverrides: wantClusterResourceOverrides,
			})
		}

		wantStatus := placementv1beta1.ClusterResourcePlacementStatus{
			Conditions:            crpOverrideFailedConditions(crp.Generation),
			PlacementStatuses:     wantPlacementStatus,
			SelectedResources:     wantSelectedResourceIdentifiers,
			ObservedResourceIndex: wantObservedResourceIndex,
		}
		if diff := cmp.Diff(crp.Status, wantStatus, crpStatusCmpOptions...); diff != "" {
			return fmt.Errorf("CRP status diff (-got, +want): %s", diff)
		}
		return nil
	}
}
func crpStatusWithWorkSynchronizedUpdatedFailedActual(
	wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier,
	wantSelectedClusters []string,
	wantObservedResourceIndex string,
	wantClusterResourceOverrides []string,
	wantResourceOverrides []placementv1beta1.NamespacedName) func() error {
	crpName := fmt.Sprintf(crpNameTemplate, GinkgoParallelProcess())

	return func() error {
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			return err
		}

		var wantPlacementStatus []placementv1beta1.ResourcePlacementStatus
		for _, name := range wantSelectedClusters {
			wantPlacementStatus = append(wantPlacementStatus, placementv1beta1.ResourcePlacementStatus{
				ClusterName:                        name,
				Conditions:                         resourcePlacementWorkSynchronizedFailedConditions(crp.Generation),
				ApplicableResourceOverrides:        wantResourceOverrides,
				ApplicableClusterResourceOverrides: wantClusterResourceOverrides,
			})
		}

		wantStatus := placementv1beta1.ClusterResourcePlacementStatus{
			Conditions:            crpWorkSynchronizedFailedConditions(crp.Generation),
			PlacementStatuses:     wantPlacementStatus,
			SelectedResources:     wantSelectedResourceIdentifiers,
			ObservedResourceIndex: wantObservedResourceIndex,
		}
		if diff := cmp.Diff(crp.Status, wantStatus, crpStatusCmpOptions...); diff != "" {
			return fmt.Errorf("CRP status diff (-got, +want): %s", diff)
		}
		return nil
	}
}

func customizedCRPStatusUpdatedActual(crpName string,
	wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier,
	wantSelectedClusters, wantUnselectedClusters []string,
	wantObservedResourceIndex string,
	resourceIsTrackable bool) func() error {
	return func() error {
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			return err
		}

		wantPlacementStatus := []placementv1beta1.ResourcePlacementStatus{}
		for _, name := range wantSelectedClusters {
			wantPlacementStatus = append(wantPlacementStatus, placementv1beta1.ResourcePlacementStatus{
				ClusterName: name,
				Conditions:  resourcePlacementRolloutCompletedConditions(crp.Generation, resourceIsTrackable, false),
			})
		}
		for i := 0; i < len(wantUnselectedClusters); i++ {
			wantPlacementStatus = append(wantPlacementStatus, placementv1beta1.ResourcePlacementStatus{
				Conditions: resourcePlacementRolloutFailedConditions(crp.Generation),
			})
		}

		var wantCRPConditions []metav1.Condition
		if len(wantSelectedClusters) > 0 {
			wantCRPConditions = crpRolloutCompletedConditions(crp.Generation, false)
		} else {
			wantCRPConditions = []metav1.Condition{
				// we don't set the remaining resource conditions.
				{
					Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             scheduler.FullyScheduledReason,
					ObservedGeneration: crp.Generation,
				},
			}
		}

		if len(wantUnselectedClusters) > 0 {
			if len(wantSelectedClusters) > 0 {
				wantCRPConditions = crpSchedulePartiallyFailedConditions(crp.Generation)
			} else {
				// we don't set the remaining resource conditions if there is no clusters to select
				wantCRPConditions = crpScheduleFailedConditions(crp.Generation)
			}
		}

		// Note that the CRP controller will only keep decisions regarding unselected clusters for a CRP if:
		//
		// * The CRP is of the PickN placement type and the required N count cannot be fulfilled; or
		// * The CRP is of the PickFixed placement type and the list of target clusters specified cannot be fulfilled.
		wantStatus := placementv1beta1.ClusterResourcePlacementStatus{
			Conditions:            wantCRPConditions,
			PlacementStatuses:     wantPlacementStatus,
			SelectedResources:     wantSelectedResourceIdentifiers,
			ObservedResourceIndex: wantObservedResourceIndex,
		}
		if diff := cmp.Diff(crp.Status, wantStatus, crpStatusCmpOptions...); diff != "" {
			return fmt.Errorf("CRP status diff (-got, +want): %s", diff)
		}
		return nil
	}
}

func safeRolloutWorkloadCRPStatusUpdatedActual(wantSelectedResourceIdentifiers []placementv1beta1.ResourceIdentifier, failedWorkloadResourceIdentifier placementv1beta1.ResourceIdentifier, wantSelectedClusters []string, wantObservedResourceIndex string, failedResourceObservedGeneration int64) func() error {
	return func() error {
		crpName := fmt.Sprintf(crpNameTemplate, GinkgoParallelProcess())
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			return err
		}

		var wantPlacementStatus []placementv1beta1.ResourcePlacementStatus
		// We only expect the deployment to not be available on one cluster.
		unavailableResourcePlacementStatus := placementv1beta1.ResourcePlacementStatus{
			Conditions: []metav1.Condition{
				{
					Type:               string(placementv1beta1.ResourceScheduledConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.ScheduleSucceededReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.RolloutStartedReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourceOverriddenConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.OverrideNotSpecifiedReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourceWorkSynchronizedConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.AllWorkSyncedReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourcesAppliedConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.AllWorkAppliedReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourcesAvailableConditionType),
					Status:             metav1.ConditionFalse,
					Reason:             condition.WorkNotAvailableReason,
					ObservedGeneration: crp.Generation,
				},
			},
			FailedPlacements: []placementv1beta1.FailedResourcePlacement{
				{
					ResourceIdentifier: failedWorkloadResourceIdentifier,
					Condition: metav1.Condition{
						Type:               string(placementv1beta1.ResourcesAvailableConditionType),
						Status:             metav1.ConditionFalse,
						Reason:             "ManifestNotAvailableYet",
						ObservedGeneration: failedResourceObservedGeneration,
					},
				},
			},
		}
		wantPlacementStatus = append(wantPlacementStatus, unavailableResourcePlacementStatus)

		// For all the other connected member clusters rollout will be blocked.
		rolloutBlockedPlacementStatus := placementv1beta1.ResourcePlacementStatus{
			Conditions: []metav1.Condition{
				{
					Type:               string(placementv1beta1.ResourceScheduledConditionType),
					Status:             metav1.ConditionTrue,
					Reason:             condition.ScheduleSucceededReason,
					ObservedGeneration: crp.Generation,
				},
				{
					Type:               string(placementv1beta1.ResourceRolloutStartedConditionType),
					Status:             metav1.ConditionFalse,
					Reason:             condition.RolloutNotStartedYetReason,
					ObservedGeneration: crp.Generation,
				},
			},
		}

		for i := 0; i < len(wantSelectedClusters)-1; i++ {
			wantPlacementStatus = append(wantPlacementStatus, rolloutBlockedPlacementStatus)
		}

		wantCRPConditions := []metav1.Condition{
			{
				Type:               string(placementv1beta1.ClusterResourcePlacementScheduledConditionType),
				Status:             metav1.ConditionTrue,
				Reason:             scheduler.FullyScheduledReason,
				ObservedGeneration: crp.Generation,
			},
			{
				Type:               string(placementv1beta1.ClusterResourcePlacementRolloutStartedConditionType),
				Status:             metav1.ConditionFalse,
				Reason:             condition.RolloutNotStartedYetReason,
				ObservedGeneration: crp.Generation,
			},
		}

		wantStatus := placementv1beta1.ClusterResourcePlacementStatus{
			Conditions:            wantCRPConditions,
			PlacementStatuses:     wantPlacementStatus,
			SelectedResources:     wantSelectedResourceIdentifiers,
			ObservedResourceIndex: wantObservedResourceIndex,
		}

		if diff := cmp.Diff(crp.Status, wantStatus, safeRolloutCRPStatusCmpOptions...); diff != "" {
			return fmt.Errorf("CRP status diff (-want, +got): %s", diff)
		}
		return nil
	}
}

func workNamespaceRemovedFromClusterActual(cluster *framework.Cluster) func() error {
	client := cluster.KubeClient

	ns := appNamespace()
	return func() error {
		if err := client.Get(ctx, types.NamespacedName{Name: ns.Name}, &corev1.Namespace{}); !errors.IsNotFound(err) {
			return fmt.Errorf("work namespace %s still exists or an unexpected error occurred: %w", ns.Name, err)
		}
		return nil
	}
}

func allFinalizersExceptForCustomDeletionBlockerRemovedFromCRPActual(crpName string) func() error {
	return func() error {
		crp := &placementv1beta1.ClusterResourcePlacement{}
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, crp); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}

		wantFinalizers := []string{customDeletionBlockerFinalizer}
		finalizer := crp.Finalizers
		if diff := cmp.Diff(finalizer, wantFinalizers); diff != "" {
			return fmt.Errorf("CRP finalizers diff (-got, +want): %s", diff)
		}

		return nil
	}
}

func crpRemovedActual(crpName string) func() error {
	return func() error {
		if err := hubClient.Get(ctx, types.NamespacedName{Name: crpName}, &placementv1beta1.ClusterResourcePlacement{}); !errors.IsNotFound(err) {
			return fmt.Errorf("CRP still exists or an unexpected error occurred: %w", err)
		}

		return nil
	}
}

func validateCRPSnapshotRevisions(crpName string, wantPolicySnapshotRevision, wantResourceSnapshotRevision int) error {
	matchingLabels := client.MatchingLabels{placementv1beta1.CRPTrackingLabel: crpName}

	snapshotList := &placementv1beta1.ClusterSchedulingPolicySnapshotList{}
	if err := hubClient.List(ctx, snapshotList, matchingLabels); err != nil {
		return err
	}
	if len(snapshotList.Items) != wantPolicySnapshotRevision {
		return fmt.Errorf("clusterSchedulingPolicySnapshotList got %v, want 1", len(snapshotList.Items))
	}
	resourceSnapshotList := &placementv1beta1.ClusterResourceSnapshotList{}
	if err := hubClient.List(ctx, resourceSnapshotList, matchingLabels); err != nil {
		return err
	}
	if len(resourceSnapshotList.Items) != wantResourceSnapshotRevision {
		return fmt.Errorf("clusterResourceSnapshotList got %v, want 2", len(snapshotList.Items))
	}
	return nil
}
