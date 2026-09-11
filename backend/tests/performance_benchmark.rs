use common::{
    lake::DuckLakeConfig,
    model::{HospitalSearchParams, PriceComparisonParams, ProcedureSearchParams},
    repository::HealthcareRepository,
};
use std::time::Instant;

const MAX_PERMITTED_LATENCY_SECS: f64 = 3.0;

#[test]
fn test_all_queries_meet_strict_performance_sla() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().unwrap();
    let catalog_path = workspace_root.join("lake/catalog.duckdb");
    if !catalog_path.exists() {
        println!("catalog.duckdb does not exist, skipping performance benchmark.");
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
    let conn = pool.get().expect("Should get connection from pool");

    println!("\n=======================================================");
    println!("     HEALTHCARE DATA API PERFORMANCE BENCHMARK SUITE   ");
    println!("       Enforcing < {:.1}s SLA on all query paths       ", MAX_PERMITTED_LATENCY_SECS);
    println!("=======================================================\n");

    let mut all_passed = true;
    let mut failed_queries = Vec::new();

    // Helper closure to measure and assert SLA
    let mut check_sla = |name: &str, f: &mut dyn FnMut() -> usize| {
        let start = Instant::now();
        let count = f();
        let duration = start.elapsed();
        let secs = duration.as_secs_f64();
        let passed = secs < MAX_PERMITTED_LATENCY_SECS;
        if !passed {
            all_passed = false;
            failed_queries.push((name.to_string(), secs));
        }
        let status = if passed { "PASSED" } else { "FAILED" };
        println!(
            "[{}] {:<55} => {:>7.2} ms ({} rows)",
            status,
            name,
            secs * 1000.0,
            count
        );
    };

    // 1. Dataset Stats
    check_sla("Dataset Stats (GET /stats)", &mut || {
        let stats = HealthcareRepository::get_dataset_stats(&conn).expect("stats");
        assert!(stats.total_hospitals > 0);
        1
    });

    // 2. Hospitals Directory - Default Page 1
    check_sla("Hospitals Page 1 (GET /hospitals?limit=18&offset=0)", &mut || {
        let res = HealthcareRepository::search_hospitals(
            &conn,
            &HospitalSearchParams {
                limit: Some(18),
                offset: Some(0),
                ..Default::default()
            },
        )
        .expect("hospitals page 1");
        assert_eq!(res.items.len(), 18);
        res.items.len()
    });

    // 3. Hospital Search by Name Query
    check_sla("Hospitals Search by Name ('General')", &mut || {
        let res = HealthcareRepository::search_hospitals(
            &conn,
            &HospitalSearchParams {
                q: Some("General".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("hospitals search");
        assert!(!res.items.is_empty());
        res.items.len()
    });

    // 4. Hospital Search by State ('TX')
    check_sla("Hospitals Filter by State ('TX')", &mut || {
        let res = HealthcareRepository::search_hospitals(
            &conn,
            &HospitalSearchParams {
                state: Some("TX".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("hospitals state");
        assert!(!res.items.is_empty());
        res.items.len()
    });

    // 5. Hospital Detail by ID (GET /hospitals/1)
    check_sla("Hospital Detail (GET /hospitals/1)", &mut || {
        let res = HealthcareRepository::get_hospital_by_id(&conn, 1).expect("hospital detail");
        assert!(res.is_some());
        1
    });

    // 6. Procedures Directory - Default Page 1 (Unfiltered)
    check_sla("Procedures Page 1 (GET /procedures?limit=20&offset=0)", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                limit: Some(20),
                offset: Some(0),
                ..Default::default()
            },
        )
        .expect("procedures page 1");
        assert_eq!(res.items.len(), 20);
        assert!(res.has_more());
        // Verify no null bytes in descriptions
        for item in &res.items {
            assert!(!item.description.contains('\0'), "Description must not contain null bytes");
        }
        res.items.len()
    });

    // 7. Procedure Search by Code (CPT 99213)
    check_sla("Procedure Search (CPT '99213')", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                code: Some("99213".to_string()),
                code_type: Some("cpt".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("procedures cpt 99213");
        assert!(!res.items.is_empty());
        res.items.len()
    });

    // 8a. Procedure Search by Code (CPT 49082 with explicit code_type)
    check_sla("Procedure Search (CPT '49082' explicit)", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                code: Some("49082".to_string()),
                code_type: Some("cpt".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("procedures cpt 49082");
        assert!(!res.items.is_empty());
        res.items.len()
    });

    // 8b. Procedure Search by Code (HCPCS 49082 with explicit code_type)
    check_sla("Procedure Search (HCPCS '49082' explicit)", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                code: Some("49082".to_string()),
                code_type: Some("hcpcs".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("procedures hcpcs 49082");
        res.items.len()
    });

    // 8c. Procedure Search by Code (49082 without code_type)
    check_sla("Procedure Search ('49082' untyped)", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                code: Some("49082".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("procedures 49082 untyped");
        res.items.len()
    });

    // 9. Procedure Search by Keyword ('knee arthroplasty')
    check_sla("Procedure Search Keyword ('knee')", &mut || {
        let res = HealthcareRepository::search_procedures(
            &conn,
            &ProcedureSearchParams {
                q: Some("knee".to_string()),
                limit: Some(20),
                ..Default::default()
            },
        )
        .expect("procedures keyword");
        assert!(!res.items.is_empty());
        res.items.len()
    });

    // 10. Price Comparison: CPT 99213 Nationwide
    check_sla("Price Compare (CPT '99213' Nationwide limit=50)", &mut || {
        let items = HealthcareRepository::compare_procedure_prices(
            &conn,
            &PriceComparisonParams {
                code: "99213".to_string(),
                code_type: Some("cpt".to_string()),
                limit: Some(50),
                ..Default::default()
            },
        )
        .expect("compare 99213 nationwide");
        assert!(!items.is_empty());
        // Verify lowest price sorting
        for w in items.windows(2) {
            let p1 = w[0].negotiated_dollar.unwrap_or(f64::MAX);
            let p2 = w[1].negotiated_dollar.unwrap_or(f64::MAX);
            assert!(p1 <= p2, "Items must be sorted ascending by negotiated dollar");
        }
        items.len()
    });

    // 11. Price Comparison: HCPCS 49082 with code_type=cpt (Interchangeable 5-digit code)
    check_sla("Price Compare ('49082' code_type=cpt limit=50)", &mut || {
        let items = HealthcareRepository::compare_procedure_prices(
            &conn,
            &PriceComparisonParams {
                code: "49082".to_string(),
                code_type: Some("cpt".to_string()),
                limit: Some(50),
                ..Default::default()
            },
        )
        .expect("compare 49082");
        assert!(!items.is_empty());
        items.len()
    });

    // 12. Price Comparison: State Filtered (CPT 99213 in 'MA')
    check_sla("Price Compare (CPT '99213' in State 'MA')", &mut || {
        let items = HealthcareRepository::compare_procedure_prices(
            &conn,
            &PriceComparisonParams {
                code: "99213".to_string(),
                state: Some("MA".to_string()),
                limit: Some(50),
                ..Default::default()
            },
        )
        .expect("compare 99213 in MA");
        assert!(!items.is_empty());
        items.len()
    });

    if !failed_queries.is_empty() {
        eprintln!("\nFailed queries exceeding {:.1}s SLA:", MAX_PERMITTED_LATENCY_SECS);
        for (name, secs) in &failed_queries {
            eprintln!("  - '{}' took {:.3}s", name, secs);
        }
        panic!("{} queries failed the performance SLA!", failed_queries.len());
    }

    println!("\n=======================================================");
    println!("  ALL 12 QUERY COMBINATIONS MET STRICT < 3.0s SLA!     ");
    println!("=======================================================\n");
}
