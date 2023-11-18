use tracing::field::{Field, ValueSet, Visit};
use tracing::level_filters::LevelFilter;
use tracing::span::{self, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

struct MatchStrVisitor<'a> {
    field:   &'a str,
    value:   &'a str,
    matched: bool,
}

impl Visit for MatchStrVisitor<'_> {
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == self.field && value == self.value {
            self.matched = true;
        }
    }
}

fn value_in_valueset(valueset: &ValueSet<'_>, field: &str, value: &str) -> bool {
    let mut visitor = MatchStrVisitor {
        field,
        value,
        matched: false,
    };
    valueset.record(&mut visitor);
    visitor.matched
}

#[allow(unused)]
fn value_in_record(record: &Record<'_>, field: &str, value: &str) -> bool {
    let mut visitor = MatchStrVisitor {
        field,
        value,
        matched: false,
    };
    record.record(&mut visitor);
    visitor.matched
}

pub struct LogFileLayer<T> {
    layer: T,
    field: String,
    value: String,
}

impl<T> LogFileLayer<T> {
    pub fn new(layer: T, field: &str, value: &str) -> LogFileLayer<T> {
        LogFileLayer {
            layer,
            field: field.to_string(),
            value: value.to_string(),
        }
    }
}

#[derive(Debug)]
struct LogFileLayerEnabled(String);

impl<S, T> Layer<S> for LogFileLayer<T>
where
    T: Layer<S>,
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_layer(&mut self, subscriber: &mut S) {
        self.layer.on_layer(subscriber)
    }

    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        self.layer.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &tracing::Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.layer.enabled(metadata, ctx)
    }

    fn event_enabled(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) -> bool {
        self.layer.event_enabled(event, ctx)
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if value_in_valueset(attrs.values(), &self.field, &self.value) {
            ctx.span(id)
                .unwrap()
                .extensions_mut()
                .insert(LogFileLayerEnabled(self.value.to_string()));
        }
        self.layer.on_new_span(attrs, id, ctx)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        self.layer.max_level_hint()
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        self.layer.on_record(span, values, ctx)
    }

    fn on_follows_from(&self, span: &span::Id, follows: &span::Id, ctx: Context<'_, S>) {
        self.layer.on_follows_from(span, follows, ctx)
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let is_on_event = if let Some(span) = ctx.event_span(event) {
            span.scope().from_root().any(|v| {
                if let Some(enabled) = v.extensions().get::<LogFileLayerEnabled>() {
                    enabled.0 == self.value
                } else {
                    false
                }
            })
        } else {
            false
        };
        if is_on_event {
            self.layer.on_event(event, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        self.layer.on_enter(id, ctx)
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        self.layer.on_exit(id, ctx)
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        self.layer.on_close(id, ctx)
    }

    unsafe fn downcast_raw(&self, id: std::any::TypeId) -> Option<*const ()> {
        self.layer.downcast_raw(id)
    }
}
