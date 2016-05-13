/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package kubectl

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/extensions"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
)

// DaemonSetRollingUpdaterConfig is the configuration for a rolling update for daemon set deployment process.
type DaemonSetRollingUpdaterConfig struct {
	// Out is a writer for progress output.
	Out io.Writer
	// OldRC is an existing controller to be replaced.
	OldDs *extensions.DaemonSet
	// NewRc is a controller that will take ownership of updated pods (will be
	// created if needed).
	NewDs *extensions.DaemonSet
	// RInterval is the time to wait between each pod recreation.
	RInterval time.Duration
	// DInterval is the time delay between daemon set creation and deletion of old one
	DInterval time.Duration
	// Timeout is the time to wait for controller updates before giving up.
	Timeout time.Duration
}

// RollingUpdater provides methods for updating replicated pods in a predictable,
// fault-tolerant way.
type DaemonSetRollingUpdater struct {
	// Client interface for creating and updating controllers
	c client.Interface
	// Namespace for resources
	ns string
}

// NewDaemonSetRollingUpdater creates a DaemonSetRollingUpdater from a client.
func NewDaemonSetRollingUpdater(namespace string, client client.Interface) *DaemonSetRollingUpdater {
	updater := &DaemonSetRollingUpdater{
		c:  client,
		ns: namespace,
	}
	return updater
}

func (r *DaemonSetRollingUpdater) Update(config *DaemonSetRollingUpdaterConfig) error {
	out := config.Out
	newDs := config.NewDs
	oldDs := config.OldDs
	rinterval := config.RInterval
	dinterval := config.DInterval
	timeout := config.Timeout

	// Create the new DS
	err := r.CreateDs(newDs, timeout, out)
	if err != nil {
		return err
	}

	time.Sleep(dinterval)

	err = r.DeleteDs(oldDs, timeout, out)
	if err != nil {
		return err
	}

	return r.RecreatePods(newDs, rinterval, timeout, out)
}

func (r *DaemonSetRollingUpdater) DeleteDs(ds *extensions.DaemonSet, timeout time.Duration, out io.Writer) error {
	// Prepare watcher filter
	dsLabelSelector, err := unversioned.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return err
	}
	fieldSelector, err := fields.ParseSelector("metadata.name=" + ds.Name)
	if err != nil {
		return err
	}
	// Watch for event with the label of the current pod
	listoptions2 := api.ListOptions{
		LabelSelector: dsLabelSelector,
		FieldSelector: fieldSelector,
	}
	// Start watcher
	watcherDelete, _ := r.c.Extensions().DaemonSets(r.ns).Watch(listoptions2)

	// Delete DaemonSet
	err = r.c.Extensions().DaemonSets(r.ns).Delete(ds.Name)
	if err != nil {
		return err
	}

	timer := time.NewTimer(timeout)
	glog.V(6).Infof("Waiting for daemon set deletion: %s\n", ds.Name)
	// Waiting for ds deletion
	deleted := false
	for !deleted {
		sleep := time.NewTimer(10 * time.Second) // In case we have no event for a while
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout waiting ds deletion %s", ds.ObjectMeta.Name)
		case <-watcherDelete.ResultChan():
		case <-sleep.C:
		}
		_, err = r.c.Extensions().DaemonSets(r.ns).Get(ds.Name)
		if err != nil {
			deleted = true
		}
	}

	glog.V(6).Infof("Daemon set deleted: %s\n", ds.Name)
	return nil
}

func (r *DaemonSetRollingUpdater) CreateDs(ds *extensions.DaemonSet, timeout time.Duration, out io.Writer) error {
	// Prepare watcher filter
	dsLabelSelector, err := unversioned.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return err
	}
	fieldSelector, err := fields.ParseSelector("metadata.name=" + ds.Name)
	if err != nil {
		return err
	}
	// Watch for event with the label of the current pod
	listoptions2 := api.ListOptions{
		LabelSelector: dsLabelSelector,
		FieldSelector: fieldSelector,
	}
	// Run watcher
	watcherCreate, _ := r.c.Extensions().DaemonSets(r.ns).Watch(listoptions2)

	// Create Daemonset
	_, err = r.c.Extensions().DaemonSets(r.ns).Create(ds)
	if err != nil {
		return err
	}

	timer := time.NewTimer(timeout)
	glog.V(6).Infof("Waiting for daemon set creation: %s\n", ds.Name)
	// Waiting for ds creation
	created := false
	for !created {
		sleep := time.NewTimer(10 * time.Second) // In case we have no event for a while
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout waiting ds creation %s", ds.ObjectMeta.Name)
		case <-watcherCreate.ResultChan():
		case <-sleep.C:
		}
		_, err = r.c.Extensions().DaemonSets(r.ns).Get(ds.Name)
		if err == nil {
			created = true
		}
	}

	glog.V(6).Infof("Daemon set created: %s\n", ds.Name)
	return nil
}

func (r *DaemonSetRollingUpdater) RecreatePods(ds *extensions.DaemonSet, rinterval, timeout time.Duration, out io.Writer) error {

	podsDeleteOptions := api.NewDeleteOptions(int64(5))
	// Get all pods from the DS
	/*
	   // We used label for backward compatibility purpose.
	   // DaemonSet selector does not have the same struct in 1.1.X and 1.2
	   // Kubectl and apiserver version can differ.
	   selector, err := extensions.LabelSelectorAsSelector(oldDs.Spec.Selector)
	   if err != nil {
	         return err
	   }
	*/
	// So we use pod template instead ... Could be dangerous...
	podDSLabelOld := labels.SelectorFromSet(labels.Set(ds.Spec.Template.Labels))

	listoptions := api.ListOptions{
		LabelSelector: podDSLabelOld,
		FieldSelector: fields.Everything(),
	}
	podOldList, err := r.c.Pods(r.ns).List(listoptions)
	if err != nil {
		return err
	}

	// Iterate on all pods
	for _, pod := range podOldList.Items {
		timer := time.NewTimer(timeout)
		// Deleting pod
		// Pod label to filter
		podLabelOld := labels.SelectorFromSet(pod.Labels)
		fieldSelector, err := fields.ParseSelector("metadata.name=" + pod.Name)
		if err != nil {
			return err
		}

		// Watch for event with the label of the current pod
		listoptions2 := api.ListOptions{
			LabelSelector: podLabelOld,
			FieldSelector: fieldSelector,
		}
		watcherDelete, _ := r.c.Pods(r.ns).Watch(listoptions2)
		// Delete pod
		r.c.Pods(r.ns).Delete(pod.ObjectMeta.Name, podsDeleteOptions)
		// Waiting for pod deletion
		glog.V(6).Infof("Waiting for pod deletion: %s\n", pod.ObjectMeta.Name)
		deleted := false
		for !deleted {
			sleep := time.NewTimer(10 * time.Second) // In case we have no event for a while
			select {
			case <-timer.C:
				return fmt.Errorf("Timeout waiting pod deletion %s", pod.ObjectMeta.Name)
			case <-watcherDelete.ResultChan():
			case <-sleep.C:
			}
			_, err = r.c.Pods(r.ns).Get(pod.Name)
			if err != nil {
				deleted = true
			}
		}
		glog.V(6).Infof("Pod deleted: %s\n", pod.ObjectMeta.Name)

		// Preparing to wait pod creation
		podlabelNew := labels.SelectorFromSet(ds.Spec.Template.Labels)
		fieldSelector2, err := fields.ParseSelector("spec.nodeName=" + pod.Spec.NodeName)

		listoptions4 := api.ListOptions{
			LabelSelector: podlabelNew,
			FieldSelector: fieldSelector2,
		}
		watcherCreate, _ := r.c.Pods(r.ns).Watch(listoptions4)

		// Waiting for pod creation
		glog.V(6).Infof("Waiting for pod creation on node: %s\n", pod.Spec.NodeName)
		running := false
		for !running {
			sleep := time.NewTimer(10 * time.Second) // In case we have no event for a while
			select {
			case <-timer.C:
				return fmt.Errorf("Timeout waiting pod creation %s", pod.ObjectMeta.Name)
			case <-watcherCreate.ResultChan():
			case <-sleep.C:
			}
			podOldList, _ = r.c.Pods(r.ns).List(listoptions4)
			for _, pod := range podOldList.Items {
				// Wait for the pod to be ready
				if api.IsPodReady(&pod) {
					running = true
				}
			}
		}
		glog.V(6).Infof("Pod ready on node: %s\n", pod.Spec.NodeName)
		time.Sleep(rinterval)
		glog.V(6).Infof("Sleep done, will loop to the next node for pod recreation")

	}
	return nil
}
