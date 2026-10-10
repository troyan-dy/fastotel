use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::resource::v1::Resource as ResourceProto;
use opentelemetry_proto::tonic::trace::v1::span::{
    Event as EventProto, Link as LinkProto, SpanKind as SpanKindProto,
};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode as StatusCodeProto;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, SpanFlags, Status as StatusProto,
};

use crate::span::{Attributes, Context, Resource, Scope, SpanData, SpanKind, StatusCode, Value};

/// Values keyed by a resource or a scope, in the order the keys first appear.
type Groups<K, V> = Vec<(Arc<K>, V)>;

/// The request body for `spans`: one `ResourceSpans` per resource and in it one `ScopeSpans` per scope, in the
/// order they first appear. Equal resources or scopes are one, as the reference exporter groups them.
pub fn encode(spans: Vec<SpanData>) -> ExportTraceServiceRequest {
    let mut resources: Groups<Resource, Groups<Option<Scope>, Vec<Span>>> = Vec::new();
    for span in spans {
        let scopes = group(&mut resources, &span.resource);
        let spans = group(scopes, &span.scope);
        spans.push(encode_span(span));
    }

    let resource_spans = resources
        .into_iter()
        .map(|(resource, scopes)| ResourceSpans {
            resource: Some(ResourceProto {
                attributes: key_values(resource.attributes.clone()),
                ..Default::default()
            }),
            scope_spans: scopes
                .into_iter()
                .map(|(scope, spans)| encode_scope_spans(scope.as_ref().as_ref(), spans))
                .collect(),
            schema_url: resource.schema_url.clone(),
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

fn group<'a, K: PartialEq, V: Default>(groups: &'a mut Groups<K, V>, key: &Arc<K>) -> &'a mut V {
    // Most spans share the copy of their resource and scope, so comparing the pointers is usually enough
    let position = groups
        .iter()
        .position(|(existing, _)| Arc::ptr_eq(existing, key) || existing == key);
    let index = match position {
        Some(index) => index,
        None => {
            groups.push((Arc::clone(key), V::default()));
            groups.len() - 1
        }
    };
    &mut groups[index].1
}

fn encode_scope_spans(scope: Option<&Scope>, spans: Vec<Span>) -> ScopeSpans {
    match scope {
        Some(scope) => ScopeSpans {
            scope: Some(InstrumentationScope {
                name: scope.name.clone(),
                version: scope.version.clone(),
                attributes: key_values(scope.attributes.clone()),
                ..Default::default()
            }),
            spans,
            schema_url: scope.schema_url.clone(),
        },
        // A span without a scope goes under an empty one, as the reference sends it
        None => ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans,
            ..Default::default()
        },
    }
}

fn encode_span(span: SpanData) -> Span {
    let kind = match span.kind {
        SpanKind::Internal => SpanKindProto::Internal,
        SpanKind::Server => SpanKindProto::Server,
        SpanKind::Client => SpanKindProto::Client,
        SpanKind::Producer => SpanKindProto::Producer,
        SpanKind::Consumer => SpanKindProto::Consumer,
    };
    let code = match span.status.code {
        StatusCode::Unset => StatusCodeProto::Unset,
        StatusCode::Ok => StatusCodeProto::Ok,
        StatusCode::Error => StatusCodeProto::Error,
    };
    Span {
        trace_id: span.trace_id.to_be_bytes().to_vec(),
        span_id: span.span_id.to_be_bytes().to_vec(),
        trace_state: span.trace_state,
        // A root span has an empty parent id
        parent_span_id: span
            .parent
            .map(|parent| parent.span_id.to_be_bytes().to_vec())
            .unwrap_or_default(),
        flags: flags(span.parent.as_ref()),
        name: span.name,
        kind: kind as i32,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span.end_time_unix_nano,
        attributes: key_values(span.attributes),
        dropped_attributes_count: span.dropped_attributes_count,
        events: span
            .events
            .into_iter()
            .map(|event| EventProto {
                time_unix_nano: event.time_unix_nano,
                name: event.name,
                attributes: key_values(event.attributes),
                dropped_attributes_count: event.dropped_attributes_count,
            })
            .collect(),
        dropped_events_count: span.dropped_events_count,
        // The reference leaves out the trace state of a link
        links: span
            .links
            .into_iter()
            .map(|link| LinkProto {
                trace_id: link.context.trace_id.to_be_bytes().to_vec(),
                span_id: link.context.span_id.to_be_bytes().to_vec(),
                attributes: key_values(link.attributes),
                dropped_attributes_count: link.dropped_attributes_count,
                flags: flags(Some(&link.context)),
                ..Default::default()
            })
            .collect(),
        dropped_links_count: span.dropped_links_count,
        status: Some(StatusProto {
            message: span.status.message,
            code: code as i32,
        }),
    }
}

/// Whether the parent or the linked span is remote, with the bit that says this is known. Like the reference,
/// it leaves the trace flags out of the low byte.
fn flags(context: Option<&Context>) -> u32 {
    let mut flags = SpanFlags::ContextHasIsRemoteMask as u32;
    if context.is_some_and(|context| context.is_remote) {
        flags |= SpanFlags::ContextIsRemoteMask as u32;
    }
    flags
}

fn key_values(attributes: Attributes) -> Vec<KeyValue> {
    attributes
        .into_iter()
        .map(|(key, value)| KeyValue {
            key,
            value: Some(any_value(value)),
            ..Default::default()
        })
        .collect()
}

fn any_value(value: Value) -> AnyValue {
    let value = match value {
        Value::Empty => None,
        Value::Bool(value) => Some(any_value::Value::BoolValue(value)),
        Value::Int(value) => Some(any_value::Value::IntValue(value)),
        Value::Double(value) => Some(any_value::Value::DoubleValue(value)),
        Value::String(value) => Some(any_value::Value::StringValue(value)),
        Value::Bytes(value) => Some(any_value::Value::BytesValue(value)),
        Value::Array(values) => Some(any_value::Value::ArrayValue(ArrayValue {
            values: values.into_iter().map(any_value).collect(),
        })),
        Value::KvList(values) => Some(any_value::Value::KvlistValue(KeyValueList {
            values: key_values(values),
        })),
    };
    AnyValue { value }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::span::{Event, Link, Status};

    fn span(name: &str, resource: &Arc<Resource>, scope: &Arc<Option<Scope>>) -> SpanData {
        SpanData {
            trace_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
            span_id: 0x1112_1314_1516_1718,
            trace_state: String::new(),
            parent: None,
            name: name.to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Status {
                code: StatusCode::Unset,
                message: String::new(),
            },
            resource: Arc::clone(resource),
            scope: Arc::clone(scope),
        }
    }

    fn resource(attributes: Attributes) -> Arc<Resource> {
        Arc::new(Resource {
            attributes,
            schema_url: String::new(),
        })
    }

    fn scope(name: &str) -> Arc<Option<Scope>> {
        Arc::new(Some(Scope {
            name: name.to_owned(),
            version: String::new(),
            attributes: vec![],
            schema_url: String::new(),
        }))
    }

    fn string(value: &str) -> Value {
        Value::String(value.to_owned())
    }

    fn only_span(request: &ExportTraceServiceRequest) -> &Span {
        &request.resource_spans[0].scope_spans[0].spans[0]
    }

    #[test]
    fn encodes_the_fields_of_a_span() {
        let resource = Arc::new(Resource {
            attributes: vec![("service.name".to_owned(), string("checkout"))],
            schema_url: "https://opentelemetry.io/schemas/1.21.0".to_owned(),
        });
        let scope = Arc::new(Some(Scope {
            name: "app".to_owned(),
            version: "1.2.3".to_owned(),
            attributes: vec![("team".to_owned(), string("cart"))],
            schema_url: "https://opentelemetry.io/schemas/1.24.0".to_owned(),
        }));
        let mut data = span("GET /", &resource, &scope);
        data.trace_state = "a=1,b=2".to_owned();
        data.parent = Some(Context {
            trace_id: data.trace_id,
            span_id: 0x2122_2324_2526_2728,
            is_remote: false,
        });
        data.kind = SpanKind::Server;
        data.attributes = vec![("http.method".to_owned(), string("GET"))];
        data.dropped_attributes_count = 3;
        data.events = vec![Event {
            name: "retry".to_owned(),
            time_unix_nano: 5,
            attributes: vec![("attempt".to_owned(), Value::Int(2))],
            dropped_attributes_count: 1,
        }];
        data.dropped_events_count = 4;
        data.links = vec![Link {
            context: Context {
                trace_id: 7,
                span_id: 8,
                is_remote: true,
            },
            attributes: vec![("kind".to_owned(), string("follows"))],
            dropped_attributes_count: 2,
        }];
        data.dropped_links_count = 5;
        data.status = Status {
            code: StatusCode::Error,
            message: "timed out".to_owned(),
        };

        let request = encode(vec![data]);

        assert_eq!(request.resource_spans.len(), 1);
        let resource_spans = &request.resource_spans[0];
        assert_eq!(
            resource_spans.schema_url,
            "https://opentelemetry.io/schemas/1.21.0"
        );
        let resource = resource_spans.resource.as_ref().unwrap();
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope_spans = &resource_spans.scope_spans[0];
        assert_eq!(
            scope_spans.schema_url,
            "https://opentelemetry.io/schemas/1.24.0"
        );
        let scope = scope_spans.scope.as_ref().unwrap();
        assert_eq!(
            (scope.name.as_str(), scope.version.as_str()),
            ("app", "1.2.3")
        );
        assert_eq!(scope.attributes[0].key, "team");
        let span = &scope_spans.spans[0];
        assert_eq!(span.trace_id, (1..=16).collect::<Vec<u8>>());
        assert_eq!(span.span_id, (0x11..=0x18).collect::<Vec<u8>>());
        assert_eq!(span.trace_state, "a=1,b=2");
        assert_eq!(span.parent_span_id, (0x21..=0x28).collect::<Vec<u8>>());
        assert_eq!(span.flags, 0x100);
        assert_eq!(span.name, "GET /");
        assert_eq!(span.kind, SpanKindProto::Server as i32);
        assert_eq!((span.start_time_unix_nano, span.end_time_unix_nano), (1, 2));
        assert_eq!(span.attributes[0].key, "http.method");
        assert_eq!(span.dropped_attributes_count, 3);
        let event = &span.events[0];
        assert_eq!(
            (
                event.name.as_str(),
                event.time_unix_nano,
                event.attributes.len()
            ),
            ("retry", 5, 1)
        );
        assert_eq!(event.dropped_attributes_count, 1);
        assert_eq!(span.dropped_events_count, 4);
        let link = &span.links[0];
        assert_eq!(link.trace_id, 7u128.to_be_bytes().to_vec());
        assert_eq!(link.span_id, 8u64.to_be_bytes().to_vec());
        assert_eq!(
            (link.attributes.len(), link.dropped_attributes_count),
            (1, 2)
        );
        assert_eq!(link.flags, 0x300);
        assert_eq!(span.dropped_links_count, 5);
        let status = span.status.as_ref().unwrap();
        assert_eq!(
            (status.code, status.message.as_str()),
            (StatusCodeProto::Error as i32, "timed out")
        );
    }

    #[test]
    fn a_root_span_has_an_empty_parent_id_and_is_not_remote() {
        let request = encode(vec![span("root", &resource(vec![]), &scope("app"))]);
        let span = only_span(&request);
        assert!(span.parent_span_id.is_empty());
        assert_eq!(span.flags, 0x100);
    }

    #[test]
    fn a_remote_parent_sets_the_is_remote_bit() {
        let mut data = span("child", &resource(vec![]), &scope("app"));
        data.parent = Some(Context {
            trace_id: data.trace_id,
            span_id: 1,
            is_remote: true,
        });
        assert_eq!(only_span(&encode(vec![data])).flags, 0x300);
    }

    #[test]
    fn encodes_attribute_values_of_every_type() {
        let mut data = span("values", &resource(vec![]), &scope("app"));
        let values = [
            Value::Empty,
            Value::Bool(true),
            Value::Int(i64::MIN),
            Value::Double(f64::NAN),
            string("é"),
            Value::Bytes(vec![0, 255]),
            Value::Array(vec![Value::Int(1), string("two"), Value::Empty]),
            Value::KvList(vec![("nested".to_owned(), Value::Array(vec![]))]),
        ];
        data.attributes = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value))
            .collect();

        let request = encode(vec![data]);

        let encoded: Vec<Option<any_value::Value>> = only_span(&request)
            .attributes
            .iter()
            .map(|key_value| key_value.value.clone().unwrap().value)
            .collect();
        let Some(any_value::Value::DoubleValue(nan)) = encoded[3] else {
            panic!("not a double: {:?}", encoded[3]);
        };
        assert!(nan.is_nan());
        let string = |value: &str| Some(any_value::Value::StringValue(value.to_owned()));
        let int = |value: i64| AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        };
        assert_eq!(
            [&encoded[..3], &encoded[4..]].concat(),
            vec![
                None,
                Some(any_value::Value::BoolValue(true)),
                Some(any_value::Value::IntValue(i64::MIN)),
                string("é"),
                Some(any_value::Value::BytesValue(vec![0, 255])),
                Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        int(1),
                        AnyValue {
                            value: string("two")
                        },
                        AnyValue { value: None }
                    ]
                })),
                Some(any_value::Value::KvlistValue(KeyValueList {
                    values: vec![KeyValue {
                        key: "nested".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::ArrayValue(ArrayValue {
                                values: vec![]
                            }))
                        }),
                        ..Default::default()
                    }]
                })),
            ]
        );
    }

    #[test]
    fn groups_equal_resources_then_equal_scopes_in_order_of_appearance() {
        let named = |name: &str| resource(vec![("service.name".to_owned(), string(name))]);
        let (cart, payments) = (named("cart"), named("payments"));
        let spans = vec![
            span("1", &cart, &scope("a")),
            span("2", &cart, &scope("b")),
            span("3", &payments, &scope("a")),
            // Equal to the resource and the scope of the first span, but other copies of them
            span("4", &named("cart"), &scope("a")),
        ];

        assert_eq!(
            names(&encode(spans)),
            vec![
                vec![owned("a", &["1", "4"]), owned("b", &["2"])],
                vec![owned("a", &["3"])]
            ]
        );
    }

    #[test]
    fn groups_as_the_sdk_compares_resources_and_scopes() {
        let attributes = || {
            vec![
                ("x".to_owned(), Value::Double(f64::NAN)),
                ("y".to_owned(), Value::Int(1)),
            ]
        };
        let reversed = || attributes().into_iter().rev().collect::<Attributes>();
        let with_attributes = |attributes: Attributes| {
            Arc::new(Some(Scope {
                name: "a".to_owned(),
                version: String::new(),
                attributes,
                schema_url: String::new(),
            }))
        };
        let spans = vec![
            span("1", &resource(attributes()), &with_attributes(attributes())),
            // Dicts are equal whatever the order of their keys, and a NaN copied twice is still one value
            span("2", &resource(reversed()), &with_attributes(reversed())),
            span("3", &resource(attributes()), &with_attributes(vec![])),
            span("4", &resource(attributes()), &Arc::new(None)),
        ];

        let request = encode(spans);

        assert_eq!(request.resource_spans.len(), 1);
        let scopes = &request.resource_spans[0].scope_spans;
        let sizes: Vec<usize> = scopes.iter().map(|scope| scope.spans.len()).collect();
        assert_eq!(sizes, vec![2, 1, 1]);
        // The scope of a span created without a tracer is empty
        assert_eq!(scopes[2].scope, Some(InstrumentationScope::default()));
    }

    fn names(request: &ExportTraceServiceRequest) -> Vec<Vec<(String, Vec<String>)>> {
        request
            .resource_spans
            .iter()
            .map(|resource| {
                resource
                    .scope_spans
                    .iter()
                    .map(|scope| {
                        let name = scope.scope.as_ref().unwrap().name.clone();
                        (
                            name,
                            scope.spans.iter().map(|span| span.name.clone()).collect(),
                        )
                    })
                    .collect()
            })
            .collect()
    }

    fn owned(scope: &str, spans: &[&str]) -> (String, Vec<String>) {
        (
            scope.to_owned(),
            spans.iter().map(|s| s.to_string()).collect(),
        )
    }
}
