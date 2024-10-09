package updaterun

import (
	"context"

	placementv1alpha1 "go.goms.io/fleet/apis/placement/v1alpha1"
	placementv1beta1 "go.goms.io/fleet/apis/placement/v1beta1"
)

// executeUpdateRun executes the update run by updating the clusters in the updatingClusterIndices with the update run.
func (r *Reconciler) executeUpdateRun(ctx context.Context, updateRun *placementv1alpha1.StagedUpdateRun, updatingStageIndex int, tobeUpdatedBinding []*placementv1beta1.ClusterResourceBinding) error {
	return nil
}
