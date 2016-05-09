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

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/extensions"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/watch"
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
	// Waiting for ds deletion
	var event watch.Event
	var event_obj *extensions.DaemonSet
	select {
	case <-timer.C:
		return fmt.Errorf("Timeout waiting ds deletion %s", ds.ObjectMeta.Name)
	case event = <-watcherDelete.ResultChan():
		event_obj, _ = event.Object.(*extensions.DaemonSet)
//		fmt.Printf("\n\nDELETED\nEVENT: %s\n", event)
//		fmt.Printf("EVENT OBJ: %s\n", event_obj)
	}
	for event.Type != watch.Deleted || event_obj.Name != ds.ObjectMeta.Name {
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout waiting ds deletion %s", ds.ObjectMeta.Name)
		case event = <-watcherDelete.ResultChan():
			event_obj, _ = event.Object.(*extensions.DaemonSet)
//			fmt.Printf("\n\nDELETED\nEVENT: %s\n", event)
//			fmt.Printf("EVENT OBJ: %s\n", event_obj)
		}
	}

	fmt.Fprintf(out, "Deleted %s\n", ds.Name)
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
	// Waiting for ds creation
	var event watch.Event
	var event_obj *extensions.DaemonSet
	select {
	case <-timer.C:
		return fmt.Errorf("Timeout waiting ds creation %s", ds.ObjectMeta.Name)
	case event = <-watcherCreate.ResultChan():
		event_obj, _ = event.Object.(*extensions.DaemonSet)
//		fmt.Printf("\n\nADDED\nEVENT: %s\n", event)
//		fmt.Printf("EVENT OBJ: %s\n", event_obj)
	}
	for event.Type != watch.Added || event_obj.Name != ds.ObjectMeta.Name {
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout waiting ds creation %s", ds.ObjectMeta.Name)
		case event = <-watcherCreate.ResultChan():
			event_obj, _ = event.Object.(*extensions.DaemonSet)
//			fmt.Printf("\n\nADDED\nEVENT: %s\n", event)
//			fmt.Printf("EVENT OBJ: %s\n", event_obj)
		}
	}

	fmt.Fprintf(out, "Created %s\n", ds.Name)
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
		var event watch.Event
		var event_obj *api.Pod
		select {
		case <-timer.C:
			return fmt.Errorf("Timeout waiting pod deletion %s", pod.ObjectMeta.Name)
		case event = <-watcherDelete.ResultChan():
			event_obj, _ = event.Object.(*api.Pod)
//			fmt.Printf("\n\nDELETED POD\nEVENT: %s\n", event)
//			fmt.Printf("EVENT OBJ: %s\n", event_obj)
		}
		for event.Type != watch.Deleted || event_obj.Name != pod.ObjectMeta.Name {
			select {
			case <-timer.C:
				return fmt.Errorf("Timeout waiting pod deletion %s", pod.ObjectMeta.Name)
			case event = <-watcherDelete.ResultChan():
				event_obj, _ = event.Object.(*api.Pod)
//				fmt.Printf("\n\nPOD DELETED\nEVENT: %s\n", event)
//				fmt.Printf("EVENT OBJ: %s\n", event_obj)
			}
		}
		// Preparing to wait pod creation
		podlabelNew := labels.SelectorFromSet(ds.Spec.Template.Labels)
		fieldSelector2, err := fields.ParseSelector("spec.nodeName=" + pod.Spec.NodeName)

		listoptions4 := api.ListOptions{
			LabelSelector: podlabelNew,
			FieldSelector: fieldSelector2,
		}
		watcherCreate, _ := r.c.Pods(r.ns).Watch(listoptions4)

		// Waiting for pod creation
		running := false
		for !running {
			select {
			case <-timer.C:
				return fmt.Errorf("Timeout waiting pod creation %s", pod.ObjectMeta.Name)
			case <-watcherCreate.ResultChan():
			}
			podOldList, _ = r.c.Pods(r.ns).List(listoptions4)
			for _, pod := range podOldList.Items {
				// Wait for the pod to be ready
				if api.IsPodReady(&pod) {
					running = true
				}
			}
		}

		time.Sleep(rinterval)

	}
	return nil
}
