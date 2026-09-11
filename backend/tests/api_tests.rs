use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use backend::{openapi::ApiDoc, routes::create_router};
use common::{
    lake::DuckLakeConfig,
    model::{HospitalSearchParams, PriceComparisonParams, ProcedureSearchParams},
    repository::HealthcareRepository,
    state::AppState,
};
use http_body_util::BodyExt;
use tower::ServiceExt;
use utoipa::OpenApi;

#[test]
fn test_openapi_spec_structure() {
    let spec = ApiDoc::openapi();
    let json_str = spec.to_pretty_json().expect("OpenAPI should serialize to valid JSON");
    let json: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");

    assert_eq!(json["info"]["title"], "Healthcare Price Transparency API");
    assert_eq!(json["info"]["version"], "1.0.0");

    let paths = &json["paths"];
    assert!(paths.get("/api/v1/hospitals").is_some());
    assert!(paths.get("/api/v1/hospitals/{id}").is_some());
    assert!(paths.get("/api/v1/procedures").is_some());
    assert!(paths.get("/api/v1/prices/compare").is_some());
    assert!(paths.get("/api/v1/stats").is_some());

    let schemas = &json["components"]["schemas"];
    assert!(schemas.get("HospitalSummary").is_some());
    assert!(schemas.get("HospitalDetail").is_some());
    assert!(schemas.get("StandardCharge").is_some());
    assert!(schemas.get("PriceComparisonItem").is_some());
    assert!(schemas.get("DatasetStats").is_some());
}

#[tokio::test]
async fn test_openapi_json_endpoint() {
    let config = DuckLakeConfig {
        catalog_path: std::path::PathBuf::from("nonexistent.duckdb"),
        lake_metadata_path: None,
        data_path: None,
        max_connections: 2,
        load_ducklake_extension: false,
    };
    let state = AppState::new(config).expect("In-memory DuckDB fallback should initialize");
    let app = create_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["info"]["title"], "Healthcare Price Transparency API");
}

#[test]
fn test_real_extracted_lake_queries() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().unwrap();
    let catalog_path = workspace_root.join("lake/catalog.duckdb");
    if !catalog_path.exists() {
        println!("catalog.duckdb does not exist at {:?}, skipping real lake test.", catalog_path);
        return;
    }
    let config = DuckLakeConfig {
        catalog_path: catalog_path.clone(),
        lake_metadata_path: Some(workspace_root.join("lake/metadata.ducklake")),
        data_path: Some(workspace_root.join("lake/data")),
        max_connections: 2,
        load_ducklake_extension: true,
    };
    let pool = common::lake::create_lake_pool(config).expect("Pool should create");
    let conn = pool.get().expect("Should get connection from lake pool");

    // 1. Stats query
    let stats = HealthcareRepository::get_dataset_stats(&conn).expect("Get dataset stats");
    println!("Stats: total_hospitals={}, states={}", stats.total_hospitals, stats.states_covered);
    assert_eq!(stats.total_hospitals, 7916);
    assert!(stats.states_covered >= 50);

    // 2. Search hospitals query
    let search_res = HealthcareRepository::search_hospitals(
        &conn,
        &HospitalSearchParams {
            q: Some("General".to_string()),
            state: Some("MA".to_string()),
            limit: Some(5),
            offset: Some(0),
            ..Default::default()
        },
    )
    .expect("Search hospitals");
    println!("Found {} Massachusetts General hospitals", search_res.total().unwrap_or(0));
    assert!(search_res.total().unwrap_or(0) > 0);
    for h in &search_res.items {
        println!("  - Hospital #{} | {} | {}, {}", h.hospital_id, h.hospital_name, h.hospital_city.as_deref().unwrap_or("N/A"), h.hospital_state.as_deref().unwrap_or(""));
    }

    // 3. Search procedures query
    let proc_res = HealthcareRepository::search_procedures(
        &conn,
        &ProcedureSearchParams {
            code: Some("99213".to_string()),
            limit: Some(3),
            ..Default::default()
        },
    )
    .expect("Search procedures");
    println!("Found procedures matching CPT 99213 (has_more={})", proc_res.has_more());
    for p in &proc_res.items {
        println!("  - Charge #{}: {} | Cash: ${:?}", p.charge_id, p.description, p.discounted_cash);
    }

    // 4. Compare prices query
    let compare_res = HealthcareRepository::compare_procedure_prices(
        &conn,
        &PriceComparisonParams {
            code: "99213".to_string(),
            state: Some("MA".to_string()),
            limit: Some(3),
            ..Default::default()
        },
    )
    .expect("Compare procedure prices");
    println!("Price comparisons for CPT 99213 in MA (count: {}):", compare_res.len());
    assert!(!compare_res.is_empty(), "Must find CPT 99213 records in MA");
    for item in &compare_res {
        println!(
            "  - {} ({}): Payer: {} ({:?}) => ${:?} (Cash: ${:?})",
            item.hospital_name,
            item.hospital_state.as_deref().unwrap_or(""),
            item.payer_name.as_deref().unwrap_or("Unknown"),
            item.plan_name.as_deref().unwrap_or(""),
            item.negotiated_dollar,
            item.discounted_cash
        );
        assert_eq!(item.hospital_state.as_deref(), Some("MA"), "Returned items must strictly match the selected state");
    }
}
