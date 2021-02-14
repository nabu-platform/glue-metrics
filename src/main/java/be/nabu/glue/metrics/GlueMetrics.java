package be.nabu.glue.metrics;

import java.nio.file.FileStore;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import be.nabu.glue.annotations.GlueParam;
import be.nabu.glue.core.api.Lambda;
import be.nabu.glue.core.impl.GlueUtils;
import be.nabu.glue.utils.ScriptRuntime;
import be.nabu.libs.evaluator.annotations.MethodProviderClass;
import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.events.api.EventHandler;
import be.nabu.libs.events.api.EventSubscription;
import be.nabu.libs.events.filters.AndEventFilter;
import be.nabu.libs.metrics.api.MetricInstance;
import be.nabu.libs.metrics.api.MetricProvider;
import be.nabu.libs.metrics.core.MetricInstanceImpl;
import be.nabu.libs.metrics.core.api.SinkEvent;
import be.nabu.libs.metrics.core.filters.SustainedThresholdFilter;
import be.nabu.libs.metrics.core.filters.ThresholdFilter;
import be.nabu.libs.metrics.impl.MetricGrouper;
import be.nabu.libs.metrics.impl.SystemMetrics;
import be.nabu.utils.cep.api.EventSeverity;
import be.nabu.utils.cep.impl.ComplexEventImpl;

@MethodProviderClass(namespace = "metrics")
public class GlueMetrics {
	
	// we need a dispatcher to listen to events
	private EventDispatcher metricsDispatcher;
	private MetricProvider provider;
	private EventDispatcher complexEventDispatcher;

	public GlueMetrics(MetricProvider provider, EventDispatcher metricsDispatcher, EventDispatcher complexEventDispatcher) {
		this.provider = provider;
		this.metricsDispatcher = metricsDispatcher;
		this.complexEventDispatcher = complexEventDispatcher;
	}
	
	/**
	 * 	Example:
	 * 
		# We know that there are gauge events every 5 seconds
		# If we have a sustained CPU problem, we want to send alerts
		# we set a minimum of 50 events which, at 5 second intervals comes down to 250s = 4 minutes of data
		sustainedThreshold (
				id: "system",
				category: "load", 
				threshold: 75, 
				duration: 5 * 60000, 
				variation: 0.15,
				minimum: 50,
				action: lambda(fireEvent(type: "ERROR", eventName: "load"))
			)
	 */
	@SuppressWarnings("unchecked")
	public EventSubscription<SinkEvent, Void> sustainedThreshold (
			@GlueParam(name = "id") final String id, 
			@GlueParam(name = "category") final String category, 
			@GlueParam(name = "threshold") final long threshold, 
			@GlueParam(name = "duration") final Long duration,
			@GlueParam(name = "amount") final Integer amount,
			@GlueParam(name = "variation") final Double variation, 
			@GlueParam(name = "minimum") final Integer minimumValues,
			@GlueParam(name = "interval") final Long interval,
			@GlueParam(name = "action") final Lambda action) {
		final ScriptRuntime fork = ScriptRuntime.getRuntime().fork(true);
		EventSubscription<SinkEvent, Void> subscription = metricsDispatcher.subscribe(SinkEvent.class, new EventHandler<SinkEvent, Void>() {
			@Override
			public Void handle(SinkEvent event) {
				GlueUtils.calculate(action, fork.fork(true), Arrays.asList(id, category, event.getTimestamp(), event.getValue()));
				return null;
			}
		});
		subscription.filter(new AndEventFilter<SinkEvent>(new EventHandler<SinkEvent, Boolean>() {
			@Override
			public Boolean handle(SinkEvent event) {
				if (id != null && !id.equals(event.getId()) && !event.getId().startsWith(id + ".")) {
					return true;
				}
				if (category != null && !category.equals(event.getCategory())) {
					return true;
				}
				return false;
			}
		}, amount == null
			? new SustainedThresholdFilter(threshold, true, duration, variation == null ? 0 : variation, minimumValues == null ? 0 : minimumValues, interval == null ? 0l : interval)
			: new SustainedThresholdFilter(threshold, true, amount, variation == null ? 0 : variation, minimumValues == null ? 0 : minimumValues, interval == null ? 0l : interval)));
		if (id != null) {
			enableMetricsFor(id);
		}
		return subscription;
	}

	// somewhat dirty, need interface for this
	private void enableMetricsFor(final String id) {
		MetricInstance metricInstance = provider.getMetricInstance(id);
		if (metricInstance instanceof MetricGrouper) {
			metricInstance = ((MetricGrouper) metricInstance).getParent();
		}
		if (metricInstance instanceof MetricInstanceImpl) {
			((MetricInstanceImpl) metricInstance).setEnableEvents(true);
		}
	}
	
	@SuppressWarnings("unchecked")
	public EventSubscription<SinkEvent, Void> threshold (
			@GlueParam(name = "id") final String id, 
			@GlueParam(name = "category") final String category, 
			@GlueParam(name = "threshold") final long threshold, 
			@GlueParam(name = "interval") final Long interval,
			@GlueParam(name = "action") final Lambda action) {
		final ScriptRuntime fork = ScriptRuntime.getRuntime().fork(true);
		EventSubscription<SinkEvent, Void> subscription = metricsDispatcher.subscribe(SinkEvent.class, new EventHandler<SinkEvent, Void>() {
			@Override
			public Void handle(SinkEvent event) {
				try {
					GlueUtils.calculate(action, fork.fork(true), Arrays.asList(id, category, event.getTimestamp(), event.getValue()));
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
		subscription.filter(new AndEventFilter<SinkEvent>(new EventHandler<SinkEvent, Boolean>() {
			@Override
			public Boolean handle(SinkEvent event) {
				if (id != null && !id.equals(event.getId()) && !event.getId().startsWith(id + ".")) {
					return true;
				}
				if (category != null && !category.equals(event.getCategory())) {
					return true;
				}
				return false;
			}
		}, new ThresholdFilter(threshold, true, interval == null ? 0 : interval)));
		if (id != null) {
			enableMetricsFor(id);
		}
		return subscription;
	}
	
	public void fireEvent(
			@GlueParam(name = "id") String id, 
			@GlueParam(name = "name") String eventName,
			@GlueParam(name = "category") String eventCategory,
			@GlueParam(name = "severity") EventSeverity severity, 
			@GlueParam(name = "message") String message,
			@GlueParam(name = "code") String code) {
		
		ComplexEventImpl event = new ComplexEventImpl();
		event.setArtifactId(id);
		event.setEventCategory(eventCategory == null ? "metric-rules" : eventCategory);
		event.setEventName(eventName);
		event.setSeverity(severity == null ? EventSeverity.INFO : severity);
		event.setMessage(message);
		event.setCreated(new Date());
		event.setCode(code);
		complexEventDispatcher.fire(event, this);
	}
	
	public Map<String, FileStore> filestores() {
		return SystemMetrics.filestoreUsageMetrics();
	}
}
