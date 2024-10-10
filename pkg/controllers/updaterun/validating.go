package updaterun

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	placementv1alpha1 "go.goms.io/fleet/apis/placement/v1alpha1"
	placementv1beta1 "go.goms.io/fleet/apis/placement/v1beta1"
	"go.goms.io/fleet/pkg/utils/condition"
	"go.goms.io/fleet/pkg/utils/controller"
)

var errStagedUpdatedAborted = fmt.Errorf("failed to continue the StagedUpdateRun")

// validateStagedUpdateRun validates the stagedUpdateRun status and ensures the update can be continued.
func (r *Reconciler) validateStagedUpdateRun(ctx context.Context, currentUpdateRun *placementv1alpha1.StagedUpdateRun) (int, []*placementv1beta1.ClusterResourceBinding, error) {
	// some of the validating function changes the object, so we need to make a copy of the object
	updateRunRef := klog.KObj(currentUpdateRun)
	updateRunCopy := currentUpdateRun.DeepCopy()
	var tobeUpdatedBinding []*placementv1beta1.ClusterResourceBinding
	klog.V(2).InfoS("start to validate the stage update run", "stagedUpdateRun", updateRunRef)
	// Validate the ClusterResourcePlacement object referenced by the StagedUpdateRun
	placementName, err := r.validateCRP(ctx, updateRunCopy)
	if err != nil {
		return -1, nil, err
	}
	// Record the latest policy snapshot associated with the ClusterResourcePlacement
	latestPolicySnapshot, nodeCount, err := r.determinePolicySnapshot(ctx, placementName, updateRunCopy)
	if err != nil {
		return -1, nil, err
	}
	// make sure the policy snapshot index used in the stagedUpdateRun is still valid
	if currentUpdateRun.Status.PolicySnapshotIndexUsed != latestPolicySnapshot.Name {
		misMatchErr := fmt.Errorf("the policy snapshot index used in the stagedUpdateRun is outdated, latest: %s, existing: %s", latestPolicySnapshot.Name, currentUpdateRun.Status.PolicySnapshotIndexUsed)
		klog.ErrorS(misMatchErr, "there is a new latest policy snapshot", "clusterResourcePlacement", placementName, "stagedUpdateRun", updateRunRef)
		return -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
	}
	// make sure the node count used in the stagedUpdateRun has not changed
	if currentUpdateRun.Status.PolicyObservedNodeCount != nodeCount {
		misMatchErr := fmt.Errorf("the node count used in the stagedUpdateRun is outdated, latest: %d, existing: %d", nodeCount, currentUpdateRun.Status.PolicyObservedNodeCount)
		klog.ErrorS(misMatchErr, "The pick N node count has changed", "clusterResourcePlacement", placementName, "stagedUpdateRun", updateRunRef)
		return -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
	}
	// Collect the scheduled clusters by the corresponding ClusterResourcePlacement with the latest policy snapshot
	scheduledBinding, tobeDeleted, err := r.collectScheduledClusters(ctx, placementName, latestPolicySnapshot, updateRunCopy)
	if err != nil {
		return -1, nil, err
	}
	// validate the applyStrategy and stagedUpdateStrategySnapshot
	if currentUpdateRun.Status.ApplyStrategy == nil {
		missingErr := fmt.Errorf("the updateRun has no applyStrategy")
		klog.ErrorS(controller.NewUnexpectedBehaviorError(missingErr), "Failed to find the applyStrategy", "clusterResourcePlacement", placementName, "stagedUpdateRun", updateRunRef)
		return -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, missingErr.Error())
	}
	if currentUpdateRun.Status.StagedUpdateStrategySnapshot == nil {
		missingErr := fmt.Errorf("the updateRun has no stagedUpdateStrategySnapshot")
		klog.ErrorS(controller.NewUnexpectedBehaviorError(missingErr), "Failed to find the stagedUpdateStrategySnapshot", "clusterResourcePlacement", placementName, "stagedUpdateRun", updateRunRef)
		return -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, missingErr.Error())
	}
	// check if the updateRun has started
	if condition.IsConditionStatusFalse(meta.FindStatusCondition(currentUpdateRun.Status.Conditions, string(placementv1alpha1.StageUpdatingConditionProgressing)), currentUpdateRun.Generation) {
		klog.V(2).InfoS("start to validate the stage update run", "stagedUpdateRun", updateRunRef)
		return 0, nil, nil
	}

	// validate the stages in the updateRun are still the same
	updatingStageIndex, updatingClusters, err := r.validateStages(ctx, scheduledBinding, tobeDeleted, updateRunCopy)
	if err != nil {
		return -1, nil, err
	}
	// We don't allow more than one cluster to be updating at the same time for now
	if len(updatingClusters) > 1 {
		dupErr := fmt.Errorf("more than one updating cluster in stage `%s`, updating clusrters: %v", updateRunCopy.Status.StagesStatus[updatingStageIndex].StageName, updatingClusters)
		klog.ErrorS(dupErr, "Detected more than one updating stage", "stagedUpdateRun", klog.KObj(currentUpdateRun))
		return -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, dupErr.Error())
	}
	if len(updatingClusters) == 1 {
		targetCluster := updatingClusters[0]
		for _, binding := range scheduledBinding {
			if binding.Spec.TargetCluster == targetCluster {
				tobeUpdatedBinding = append(tobeUpdatedBinding, binding)
				return updatingStageIndex, tobeUpdatedBinding, err
			}
		}
		for _, binding := range tobeDeleted {
			if binding.Spec.TargetCluster != targetCluster {
				tobeUpdatedBinding = append(tobeUpdatedBinding, binding)
				return updatingStageIndex, tobeUpdatedBinding, err
			}
		}
	}
	return updatingStageIndex, nil, err
}

// validateStages validates the stages in the updateRun and returns the stage index and the next cluster to be updated.
// The function returns the stage index that is updating, the clusters that are updating, and any error that is encountered.
// if the updating stage index is -1, it means all stages are finished, and the updateRun should be finished.
// if the updating stage index is 0, the next stage to be updated will be the first stage.
// if the updating stage index is len(updateRun.Status.StagesStatus), the next stage to be updated will be the delete stage.
func (r *Reconciler) validateStages(ctx context.Context, scheduledBinding, tobeDeletedBindings []*placementv1beta1.ClusterResourceBinding, updateRun *placementv1alpha1.StagedUpdateRun) (int, []string, error) {
	// take a copy of the existing updateRun
	existingStageStatus := updateRun.Status.DeepCopy().StagesStatus
	existingDeleteStageStatus := updateRun.Status.DeletionStageStatus.DeepCopy()
	// compute the stage status, not including the delete stage
	if err := r.computeRunStageStatus(ctx, scheduledBinding, updateRun); err != nil {
		return -1, nil, err
	}
	// validate the stages in the updateRun and return the updating stage index
	updatingStageIndex, lastFinishedStageIndex, updatingClusters, validateErr := validateStageStatus(existingStageStatus, updateRun)
	if validateErr != nil {
		return -1, nil, validateErr
	}
	// check if the delete stage is active
	deleteStageActive, validateErr := validateDeleteStage(existingDeleteStageStatus, tobeDeletedBindings, updateRun, updatingStageIndex, lastFinishedStageIndex)
	if validateErr != nil {
		// return error if the delete stage is active and there is an error
		return -1, nil, validateErr
	}
	if !deleteStageActive {
		if updatingStageIndex == -1 && lastFinishedStageIndex == len(updateRun.Status.StagesStatus)-1 {
			// this means all stages are finished, the updateRun should be finished
			return -1, nil, nil
		}
	} else {
		// the delete stage is active, return the deleting clusters
		return len(updateRun.Status.StagesStatus), nil, nil
	}
	// if no stage is updating, continue from the last finished stage (which will result it start from 0)
	// if all stages are finished, continue from the delete stage
	if updatingStageIndex == -1 {
		updatingStageIndex = lastFinishedStageIndex + 1
	}
	return updatingStageIndex, updatingClusters, nil
}

// validateStageStatus is a helper function to validate the updating stages in the updateRun.
func validateStageStatus(existingStageStatus []placementv1alpha1.StageUpdatingStatus, updateRun *placementv1alpha1.StagedUpdateRun) (int, int, []string, error) {
	updatingStageIndex := -1
	lastFinishedStageIndex := -1
	var updatingClusters []string
	newStageStatus := updateRun.Status.StagesStatus
	// make sure the stages in the updateRun are still the same
	if len(existingStageStatus) != len(newStageStatus) {
		misMatchErr := fmt.Errorf("the number of stages in the stagedUpdateRun has changed, latest: %d, existing: %d", len(newStageStatus), len(existingStageStatus))
		klog.ErrorS(misMatchErr, "The number of stages has changed", "stagedUpdateRun", klog.KObj(updateRun))
		return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
	}
	// make sure the stages in the updateRun are still the same
	for curStage := range existingStageStatus {
		if existingStageStatus[curStage].StageName != newStageStatus[curStage].StageName {
			misMatchErr := fmt.Errorf("the `%d` stage in the stagedUpdateRun has changed, latest: %s, existing: %s", curStage, newStageStatus[curStage].StageName, existingStageStatus[curStage].StageName)
			klog.ErrorS(misMatchErr, "The stage  has changed", "stagedUpdateRun", klog.KObj(updateRun))
			return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
		}
		if len(existingStageStatus[curStage].Clusters) != len(newStageStatus[curStage].Clusters) {
			misMatchErr := fmt.Errorf("the number of clusters in the stage `%s` has changed, latest: %d, existing: %d", existingStageStatus[curStage].StageName, len(newStageStatus), len(existingStageStatus))
			klog.ErrorS(misMatchErr, "The number of clusters in a stage has changed", "stagedUpdateRun", klog.KObj(updateRun))
			return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
		}
		// check that the clusters in the stage are still the same
		for j := range existingStageStatus[curStage].Clusters {
			if existingStageStatus[curStage].Clusters[j].ClusterName != newStageStatus[curStage].Clusters[j].ClusterName {
				misMatchErr := fmt.Errorf("the `%d`th cluster in the stage `%s` has changed, latest: %s, existing: %s", j, existingStageStatus[curStage].StageName, newStageStatus[curStage].Clusters[j].ClusterName, existingStageStatus[curStage].Clusters[j].ClusterName)
				klog.ErrorS(misMatchErr, "The cluster in a stage has changed", "stagedUpdateRun", klog.KObj(updateRun))
				return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
			}
		}
		stageCond := meta.FindStatusCondition(existingStageStatus[curStage].Conditions, string(placementv1alpha1.StageUpdatingConditionSucceeded))
		if condition.IsConditionStatusTrue(stageCond, updateRun.Generation) { // the stage has finished
			if updatingStageIndex != -1 && curStage > updatingStageIndex {
				// the finished stage is after the updating stage
				updateErr := fmt.Errorf("the finished stage `%d` is after the updating stage `%d`", curStage, updatingStageIndex)
				klog.ErrorS(updateErr, "The finished stage is after the updating stage", "currentStage", existingStageStatus[curStage].StageName, "updatingStage", existingStageStatus[updatingStageIndex].StageName, "stagedUpdateRun", klog.KObj(updateRun))
				return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, updateErr.Error())
			}
			// record the last finished stage so we can continue from the next stage if no stage is updating
			lastFinishedStageIndex = curStage
			// make sure that all the clusters are upgraded
			for j := range existingStageStatus[curStage].Clusters {
				// check if the cluster is updating
				if condition.IsConditionStatusTrue(meta.FindStatusCondition(existingStageStatus[curStage].Clusters[j].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeStarted)), updateRun.Generation) &&
					condition.IsConditionStatusFalse(meta.FindStatusCondition(existingStageStatus[curStage].Clusters[j].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeSucceeded)), updateRun.Generation) {
					updatingClusters = append(updatingClusters, existingStageStatus[curStage].Clusters[j].ClusterName)
				}
			}
		} else if condition.IsConditionStatusFalse(stageCond, updateRun.Generation) { // the stage is failed
			failedErr := fmt.Errorf("the stage `%s` has failed, err: %s", existingStageStatus[curStage].StageName, stageCond.Message)
			klog.ErrorS(failedErr, "The stage has failed", "stageCond", stageCond, "stagedUpdateRun", klog.KObj(updateRun))
			return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, failedErr.Error())
		} else if condition.IsConditionStatusTrue(meta.FindStatusCondition(existingStageStatus[curStage].Conditions, string(placementv1alpha1.StageUpdatingConditionProgressing)), updateRun.Generation) { // the stage is updating
			// check this is the only stage that is updating
			if updatingStageIndex != -1 {
				dupErr := fmt.Errorf("more than one updating stage, previous updating stage: %s, new updating stage: %s", existingStageStatus[updatingStageIndex].StageName, existingStageStatus[curStage].StageName)
				klog.ErrorS(dupErr, "Detected more than one updating stage", "stagedUpdateRun", klog.KObj(updateRun))
				return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, dupErr.Error())
			}
			updatingStageIndex = curStage
			// collect the updating clusters
			var updatingClusterIndex []int
			lastIndex := -1
			for j := range existingStageStatus[curStage].Clusters {
				// check if the cluster is updating
				if condition.IsConditionStatusTrue(meta.FindStatusCondition(existingStageStatus[curStage].Clusters[j].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeStarted)), updateRun.Generation) &&
					condition.IsConditionStatusFalse(meta.FindStatusCondition(existingStageStatus[curStage].Clusters[j].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeSucceeded)), updateRun.Generation) {
					updatingClusters = append(updatingClusters, existingStageStatus[curStage].Clusters[j].ClusterName)
					updatingClusterIndex = append(updatingClusterIndex, j)
				}
			}
			for _, index := range updatingClusterIndex {
				// check if updating clusters are consecutive
				if lastIndex == -1 {
					lastIndex = index
				} else if index != lastIndex+1 {
					updateErr := fmt.Errorf("the updating cluster in stage `%s` are not consecutive, the updating clusrters: %v, the updating clusrter index: %v", existingStageStatus[curStage].StageName, updatingClusters, updatingClusterIndex)
					klog.ErrorS(updateErr, "Detected none consecutive updating clusters", "stagedUpdateRun", klog.KObj(updateRun))
					return -1, -1, nil, fmt.Errorf("%w: %s", errStagedUpdatedAborted, updateErr.Error())
				}
				lastIndex++
			}
		}
	}
	return updatingStageIndex, lastFinishedStageIndex, updatingClusters, nil
}

// validateDeleteStage is a helper function to validate the existing delete stage comparing with the latest list of clusters to be deleted.
// existingDeleteStageStatus is the existing delete stage status in the StagedUpdateRun
// curUpdateRun is the updateRun that has the latest list of clusters in the stages
// tobeDeletedBindings is the latest list of ClusterResourceBinding that are to be deleted
// updatingStageIndex is the index of the stage that is updating in the curUpdateRun
// lastFinishedStageIndex is the index of the last finished stage in the curUpdateRun
// This method will return if the delete stage is active and any error that is encountered.
func validateDeleteStage(existingDeleteStageStatus *placementv1alpha1.StageUpdatingStatus, tobeDeletedBindings []*placementv1beta1.ClusterResourceBinding, curUpdateRun *placementv1alpha1.StagedUpdateRun, updatingStageIndex int, lastFinishedStageIndex int) (bool, error) {
	deleteStageFinishedCond := meta.FindStatusCondition(existingDeleteStageStatus.Conditions, string(placementv1alpha1.StageUpdatingConditionSucceeded))
	deleteStageProgressingCond := meta.FindStatusCondition(existingDeleteStageStatus.Conditions, string(placementv1alpha1.StageUpdatingConditionProgressing))
	if updatingStageIndex != -1 || lastFinishedStageIndex != len(curUpdateRun.Status.StagesStatus)-1 {
		// there are still stages updating before the delete staging, make sure the delete stage is not active
		if condition.IsConditionStatusTrue(deleteStageFinishedCond, curUpdateRun.Generation) || condition.IsConditionStatusFalse(deleteStageFinishedCond, curUpdateRun.Generation) ||
			condition.IsConditionStatusTrue(deleteStageProgressingCond, curUpdateRun.Generation) {
			updateErr := fmt.Errorf("the delete stage is active, but there are still stages updating, updatingStageIndex: %d, lastFinishedStageIndex: %d", updatingStageIndex, lastFinishedStageIndex)
			klog.ErrorS(updateErr, "The delete stage is active, but there are still stages updating", "stagedUpdateRun", klog.KObj(curUpdateRun))
			return false, fmt.Errorf("%w: %s", errStagedUpdatedAborted, updateErr.Error())
		}
	}
	// check if the delete stage has finished successfully
	if condition.IsConditionStatusTrue(deleteStageFinishedCond, curUpdateRun.Generation) {
		klog.InfoS("The delete stage has finished successfully, no more stage to update", "stagedUpdateRun", klog.KObj(curUpdateRun))
		return false, nil
	}
	// check if the delete stage has failed
	if condition.IsConditionStatusFalse(deleteStageFinishedCond, curUpdateRun.Generation) {
		failedErr := fmt.Errorf("the delete stage has failed, err: %s", deleteStageFinishedCond.Message)
		klog.ErrorS(failedErr, "The delete stage has failed", "stageCond", deleteStageFinishedCond, "stagedUpdateRun", klog.KObj(curUpdateRun))
		return false, fmt.Errorf("%w: %s", errStagedUpdatedAborted, failedErr.Error())
	}
	if condition.IsConditionStatusTrue(deleteStageProgressingCond, curUpdateRun.Generation) {
		tobeDeletedBindingsMap := make(map[string]bool)
		for _, binding := range tobeDeletedBindings {
			tobeDeletedBindingsMap[binding.Spec.TargetCluster] = true
		}
		// check that the clusters in the stage are part of the tobeDeletedBindings
		for curCluster := range existingDeleteStageStatus.Clusters {
			clusterDeletedCond := meta.FindStatusCondition(existingDeleteStageStatus.Clusters[curCluster].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeSucceeded))

			// check if the cluster is deleting
			if condition.IsConditionStatusTrue(meta.FindStatusCondition(existingDeleteStageStatus.Clusters[curCluster].Conditions, string(placementv1alpha1.UpdatingStatusConditionTypeStarted)), curUpdateRun.Generation) &&
				condition.IsConditionStatusFalse(clusterDeletedCond, curUpdateRun.Generation) {
				if !tobeDeletedBindingsMap[existingDeleteStageStatus.Clusters[curCluster].ClusterName] {
					misMatchErr := fmt.Errorf("the `%d`th cluster in the deleting stage has changed, latest: %s, existing: %s", curCluster, tobeDeletedBindings[curCluster].Spec.TargetCluster, existingDeleteStageStatus.Clusters[curCluster].ClusterName)
					klog.ErrorS(misMatchErr, "The cluster in the deleting stage has changed", "stagedUpdateRun", klog.KObj(curUpdateRun))
					return true, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
				}
			}
		}
		// the existing delete clusters should be a super-set of the current to be deleted bindings
		if len(tobeDeletedBindings) > 0 {
			misMatchErr := fmt.Errorf("the number of clusters in the deleting stage has grow, bindings that are not recored : %+v", tobeDeletedBindingsMap, len(existingDeleteStageStatus.Clusters))
			klog.ErrorS(misMatchErr, "The delete stage has changed", "stagedUpdateRun", klog.KObj(curUpdateRun))
			return true, fmt.Errorf("%w: %s", errStagedUpdatedAborted, misMatchErr.Error())
		}

		klog.InfoS("The delete stage is updating", "numberOfDeletingClusters", len(updatingClusters), "stagedUpdateRun", klog.KObj(curUpdateRun))
		return true, nil
	}
	return true, nil
}

// recordUpdateRunFailed records the failed update run in the StagedUpdateRun status.
func (r *Reconciler) recordUpdateRunFailed(ctx context.Context, updateRun *placementv1alpha1.StagedUpdateRun, message string) error {
	meta.SetStatusCondition(&updateRun.Status.Conditions, metav1.Condition{
		Type:               string(placementv1alpha1.StagedUpdateRunConditionSucceeded),
		Status:             metav1.ConditionFalse,
		ObservedGeneration: updateRun.Generation,
		Reason:             condition.UpdateRunFailedReason,
		Message:            message,
	})
	if updateErr := r.Client.Status().Update(ctx, updateRun); updateErr != nil {
		klog.ErrorS(updateErr, "Failed to update the StagedUpdateRun status as failed", "stagedUpdateRun", klog.KObj(updateRun))
		return updateErr
	}
	return nil
}
