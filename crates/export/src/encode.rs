use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource as ResourceProto;
use opentelemetry_proto::tonic::trace::v1::span::SpanKind as SpanKindProto;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use crate::span::{Attributes, Resource, Scope, SpanData, SpanKind};

/// Values keyed by a resource or a scope, in the order the keys first appear.
type Groups<K, V> = Vec<(Arc<K>, V)>;

/// The request body for `spans`: one `ResourceSpans` per resource and in it one `ScopeSpans` per scope, in the
/// order they first appear. Equal resources or scopes are one, as the reference exporter groups them.
pub fn encode(spans: Vec<SpanData>) -> ExportTraceServiceRequest {
    let mut resources: Groups<Resource, Groups<Scope, Vec<Span>>> = Vec::new();
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
                .map(|(scope, spans)| ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: scope.name.clone(),
                        version: scope.version.clone(),
                        ..Default::default()
                    }),
                    spans,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
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

fn encode_span(span: SpanData) -> Span {
    let kind = match span.kind {
        SpanKind::Internal => SpanKindProto::Internal,
        SpanKind::Server => SpanKindProto::Server,
        SpanKind::Client => SpanKindProto::Client,
        SpanKind::Producer => SpanKindProto::Producer,
        SpanKind::Consumer => SpanKindProto::Consumer,
    };
    Span {
        trace_id: span.trace_id.to_be_bytes().to_vec(),
        span_id: span.span_id.to_be_bytes().to_vec(),
        // A root span has an empty parent id
        parent_span_id: span
            .parent_span_id
            .map(|id| id.to_be_bytes().to_vec())
            .unwrap_or_default(),
        name: span.name,
        kind: kind as i32,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span.end_time_unix_nano,
        attributes: key_values(span.attributes),
        ..Default::default()
    }
}

fn key_values(attributes: Attributes) -> Vec<KeyValue> {
    attributes
        .into_iter()
        .map(|(key, value)| KeyValue {
            key,
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value)),
            }),
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(name: &str, resource: &Arc<Resource>, scope: &Arc<Scope>) -> SpanData {
        SpanData {
            trace_id: 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10,
            span_id: 0x1112_1314_1516_1718,
            parent_span_id: None,
            name: name.to_owned(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes: vec![],
            resource: Arc::clone(resource),
            scope: Arc::clone(scope),
        }
    }

    fn scope(name: &str) -> Arc<Scope> {
        Arc::new(Scope {
            name: name.to_owned(),
            version: String::new(),
        })
    }

    #[test]
    fn encodes_the_fields_of_a_span() {
        let resource = Arc::new(Resource {
            attributes: vec![("service.name".to_owned(), "checkout".to_owned())],
        });
        let scope = Arc::new(Scope {
            name: "app".to_owned(),
            version: "1.2.3".to_owned(),
        });
        let mut data = span("GET /", &resource, &scope);
        data.parent_span_id = Some(0x2122_2324_2526_2728);
        data.kind = SpanKind::Server;
        data.attributes = vec![("http.method".to_owned(), "GET".to_owned())];

        let request = encode(vec![data]);

        assert_eq!(request.resource_spans.len(), 1);
        let resource_spans = &request.resource_spans[0];
        let resource = resource_spans.resource.as_ref().unwrap();
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope_spans = &resource_spans.scope_spans[0];
        let scope = scope_spans.scope.as_ref().unwrap();
        assert_eq!(
            (scope.name.as_str(), scope.version.as_str()),
            ("app", "1.2.3")
        );
        let span = &scope_spans.spans[0];
        assert_eq!(span.trace_id, (1..=16).collect::<Vec<u8>>());
        assert_eq!(span.span_id, (0x11..=0x18).collect::<Vec<u8>>());
        assert_eq!(span.parent_span_id, (0x21..=0x28).collect::<Vec<u8>>());
        assert_eq!(span.name, "GET /");
        assert_eq!(span.kind, SpanKindProto::Server as i32);
        assert_eq!((span.start_time_unix_nano, span.end_time_unix_nano), (1, 2));
        let value = span.attributes[0]
            .value
            .as_ref()
            .unwrap()
            .value
            .as_ref()
            .unwrap();
        assert_eq!(*value, any_value::Value::StringValue("GET".to_owned()));
    }

    #[test]
    fn a_root_span_has_an_empty_parent_id() {
        let request = encode(vec![span(
            "root",
            &Arc::new(Resource { attributes: vec![] }),
            &scope("app"),
        )]);
        assert!(
            request.resource_spans[0].scope_spans[0].spans[0]
                .parent_span_id
                .is_empty()
        );
    }

    #[test]
    fn groups_equal_resources_then_equal_scopes_in_order_of_appearance() {
        let resource = |name: &str| {
            Arc::new(Resource {
                attributes: vec![("service.name".to_owned(), name.to_owned())],
            })
        };
        let (cart, payments) = (resource("cart"), resource("payments"));
        let spans = vec![
            span("1", &cart, &scope("a")),
            span("2", &cart, &scope("b")),
            span("3", &payments, &scope("a")),
            // Equal to the resource and the scope of the first span, but other copies of them
            span("4", &resource("cart"), &scope("a")),
        ];

        let request = encode(spans);

        let names: Vec<Vec<(String, Vec<String>)>> = request
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
            .collect();
        let owned = |scope: &str, spans: &[&str]| {
            (
                scope.to_owned(),
                spans.iter().map(|s| s.to_string()).collect(),
            )
        };
        assert_eq!(
            names,
            vec![
                vec![owned("a", &["1", "4"]), owned("b", &["2"])],
                vec![owned("a", &["3"])]
            ]
        );
    }
}
