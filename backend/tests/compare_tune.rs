use common::lake::DuckLakeConfig;
use std::time::Instant;

#[test]
fn test_compare_tune() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().unwrap();
    let catalog_path = workspace_root.join("lake/catalog.duckdb");
    if !catalog_path.exists() {
        return;
    }
    let config = DuckLakeConfig {
        catalog_path: catalog_path.clone(),
        lake_metadata_path: Some(workspace_root.join("lake/metadata.ducklake")),
        data_path: Some(workspace_root.join("lake/data")),
        max_connections: 1,
        load_ducklake_extension: true,
    };
    let pool = common::lake::create_lake_pool(config).expect("Pool should create");
    let conn = pool.get().expect("Should get connection");

    conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS fast_hospitals AS SELECT * FROM current_hospitals;").unwrap();

    println!("\n=== INVESTIGATING 49082 PRICE COMPARISON ===");

    // Query 1: d.cpt = '49082' on current_charge_details
    let t0 = Instant::now();
    let count_cpt: i64 = conn.query_row(
        "SELECT COUNT(*) FROM current_charge_details WHERE cpt = '49082'",
        [],
        |r| r.get(0)
    ).unwrap_or(0);
    println!("d.cpt = '49082' count: {} (took {:?})", count_cpt, t0.elapsed());

    // Query 2: d.hcpcs = '49082' on current_charge_details
    let t1 = Instant::now();
    let count_hcpcs: i64 = conn.query_row(
        "SELECT COUNT(*) FROM current_charge_details WHERE hcpcs = '49082'",
        [],
        |r| r.get(0)
    ).unwrap_or(0);
    println!("d.hcpcs = '49082' count: {} (took {:?})", count_hcpcs, t1.elapsed());

    // Query 3: Full compare query on (cpt = '49082' OR hcpcs = '49082')
    let t2 = Instant::now();
    let rows: Vec<f64> = conn.prepare(
        "SELECT d.standard_charge_dollar
         FROM current_charge_details d
         WHERE (d.cpt = '49082' OR d.hcpcs = '49082') AND d.standard_charge_dollar > 0
         ORDER BY d.standard_charge_dollar ASC
         LIMIT 50;"
    ).unwrap().query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    println!("Combined cpt/hcpcs 49082: found {} rows in {:?}", rows.len(), t2.elapsed());

    // Query 4: Check if joining lake.current_hospital_versions is faster than the view
    let t3 = Instant::now();
    let rows_direct: Vec<f64> = conn.prepare(
        "SELECT scd.standard_charge_dollar
         FROM lake.standard_charge_details scd
         JOIN lake.current_hospital_versions v ON scd.internal_id = v.internal_id AND scd.run_date = v.run_date
         WHERE (scd.cpt = '49082' OR scd.hcpcs = '49082') AND scd.standard_charge_dollar > 0
         ORDER BY scd.standard_charge_dollar ASC
         LIMIT 50;"
    ).unwrap().query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    println!("Direct join on lake.standard_charge_details: found {} rows in {:?}", rows_direct.len(), t3.elapsed());

    // Query 5: Can we query from current_charges instead when details takes too long, or how to get sub-3s?
    // Let's check EXPLAIN on Query 3
    let explain_str: String = conn.query_row(
        "EXPLAIN SELECT d.standard_charge_dollar
         FROM current_charge_details d
         WHERE (d.cpt = '49082' OR d.hcpcs = '49082') AND d.standard_charge_dollar > 0
         ORDER BY d.standard_charge_dollar ASC
         LIMIT 50;",
        [],
        |r| r.get(1)
    ).unwrap_or_default();
    println!("\nEXPLAIN PLAN:\n{}", explain_str);
}
