package types

import (
	"fmt"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/dydxprotocol/v4-chain/protocol/daemons/types"
	"sync"
	"time"
)

type updateMetadata struct {
	timer           *time.Timer
	updateFrequency time.Duration
}

// HealthMonitor monitors the health of daemon services, which implement the HealthCheckable interface. If a
// registered health-checkable service sustains an unhealthy state for the maximum acceptable unhealthy duration,
// the monitor will execute a callback function.
type HealthMonitor struct {
	// serviceToUpdateMetadata maps daemon service names to their update metadata.
	serviceToUpdateMetadata map[string]updateMetadata
	// stopped indicates whether the monitor has been stopped. Additional daemon services cannot be registered
	// after the monitor has been stopped.
	stopped bool
	// disabled indicates whether the monitor has been disabled. This is used to disable the monitor in testApp
	// tests, where app.New is not executed.
	disabled bool
	// lock is used to synchronize access to the monitor.
	lock sync.Mutex

	// These fields are initialized in NewHealthMonitor and are not modified after initialization.
	logger                   log.Logger
	daemonStartupGracePeriod time.Duration
}

// NewHealthMonitor creates a new health monitor.
func NewHealthMonitor(daemonStartupGracePeriod time.Duration, logger log.Logger) *HealthMonitor {
	return &HealthMonitor{
		serviceToUpdateMetadata:  make(map[string]updateMetadata),
		logger:                   logger,
		daemonStartupGracePeriod: daemonStartupGracePeriod,
	}
}

func (ufm *HealthMonitor) DisableForTesting() {
	ufm.lock.Lock()
	defer ufm.lock.Unlock()

	ufm.disabled = true
}

// RegisterHealthCheckableWithCallback registers a HealthCheckable with the health monitor. If the service
// stays unhealthy every time it is polled during the maximum acceptable unhealthy duration, the monitor will
// execute the callback function.
// This method is synchronized. The method returns an error if the service was already registered or the
// monitor has already been stopped.
func (ufm *HealthMonitor) RegisterHealthCheckableWithCallback(
	hc types.HealthCheckable,
	maximumAcceptableUnhealthyDuration time.Duration,
	callback func(),
) error {
	ufm.lock.Lock()
	defer ufm.lock.Unlock()

	if maximumAcceptableUnhealthyDuration <= 0 {
		return fmt.Errorf(
			"health check registration failure for service %v: "+
				"maximum acceptable unhealthy duration %v must be positive",
			hc.ServiceName(),
			maximumAcceptableUnhealthyDuration,
		)
	}

	// Don't register daemon services if the monitor has been disabled.
	if ufm.disabled {
		return nil
	}

	// Don't register additional daemon services if the monitor has already been stopped.
	// This could be a concern for short-running integration test cases, where the network
	// stops before all daemon services have been registered.
	if ufm.stopped {
		return fmt.Errorf(
			"health check registration failure for service %v: monitor has been stopped",
			hc.ServiceName(),
		)
	}

	if _, ok := ufm.serviceToUpdateMetadata[hc.ServiceName()]; ok {
		return fmt.Errorf("service %v already registered", hc.ServiceName())
	}

	ufm.serviceToUpdateMetadata[hc.ServiceName()] = updateMetadata{
		timer:           time.AfterFunc(ufm.daemonStartupGracePeriod+maximumAcceptableUnhealthyDuration, callback),
		updateFrequency: maximumAcceptableUnhealthyDuration,
	}
	return nil
}

// PanicServiceNotResponding returns a function that panics with a message indicating that the specified daemon
// service is not responding. This is ideal for creating a callback function when registering a daemon service.
func PanicServiceNotResponding(service string) func() {
	return func() {
		panic(fmt.Sprintf("%v daemon not responding", service))
	}
}

// LogErrorServiceNotResponding returns a function that logs an error indicating that the specified daemon service
// is not responding. This is ideal for creating a callback function when registering a daemon service.
func LogErrorServiceNotResponding(service string, logger log.Logger) func() {
	return func() {
		logger.Error(
			"daemon not responding",
			"service",
			service,
		)
	}
}

// RegisterDaemonService registers a new daemon service with the update frequency monitor. If the daemon service
// fails to respond within the maximum acceptable update delay, the monitor will log an error.
// This method is synchronized. The method an error if the service was already registered or the monitor has
// already been stopped.
func (ufm *HealthMonitor) RegisterDaemonService(
	hc types.HealthCheckable,
	maximumAcceptableUpdateDelay time.Duration,
) error {
	return ufm.RegisterHealthCheckableWithCallback(
		hc,
		maximumAcceptableUpdateDelay,
		LogErrorServiceNotResponding(hc.ServiceName(), ufm.logger),
	)
}

// Stop stops the update frequency monitor. This method is synchronized.
func (ufm *HealthMonitor) Stop() {
	ufm.lock.Lock()
	defer ufm.lock.Unlock()

	// Don't stop the monitor if it has already been stopped.
	if ufm.stopped {
		return
	}

	for _, metadata := range ufm.serviceToUpdateMetadata {
		metadata.timer.Stop()
	}
	ufm.stopped = true
}

// RegisterValidResponse registers a valid response from the daemon service. This will reset the timer for the
// daemon service. This method is synchronized.
func (ufm *HealthMonitor) RegisterValidResponse(service string) error {
	ufm.lock.Lock()
	defer ufm.lock.Unlock()

	// Don't return an error if the monitor has been disabled.
	if ufm.disabled {
		return nil
	}

	// Don't bother to reset the timer if the monitor has already been stopped.
	if ufm.stopped {
		return nil
	}

	metadata, ok := ufm.serviceToUpdateMetadata[service]
	if !ok {
		return fmt.Errorf("service %v not registered", service)
	}

	metadata.timer.Reset(metadata.updateFrequency)
	return nil
}
