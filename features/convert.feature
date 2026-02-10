Feature: Convert

  Convert between Parquet, Avro, and CSV file formats.

  Scenario: Parquet to Avro
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/parquet_to_avro.avro`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/parquet_to_avro.avro"
    And the file "$TEMPDIR/parquet_to_avro.avro" should exist

  Scenario: Avro to Parquet
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/avro_to_parquet.parquet`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/avro_to_parquet.parquet"
    And the file "$TEMPDIR/avro_to_parquet.parquet" should exist

  Scenario: Parquet to CSV
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/parquet_to_csv.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/parquet_to_csv.csv"
    And the file "$TEMPDIR/parquet_to_csv.csv" should exist

  Scenario: Avro to CSV
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/avro_to_csv.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/avro_to_csv.csv"
    And the file "$TEMPDIR/avro_to_csv.csv" should exist

  Scenario: Parquet to CSV with --select
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/parquet_select.csv --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/parquet_select.csv"
    And the file "$TEMPDIR/parquet_select.csv" should exist

  Scenario: Avro to CSV with --select
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/avro_select.csv --select id,first_name,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/avro_select.csv"
    And the file "$TEMPDIR/avro_select.csv" should exist

  Scenario: Parquet to Avro with --limit
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/parquet_limit.avro --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/parquet_limit.avro"
    And the file "$TEMPDIR/parquet_limit.avro" should exist

  Scenario: Avro to CSV with --limit and --select
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/avro_limit_select.csv --limit 3 --select id,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/avro_limit_select.csv"
    And the file "$TEMPDIR/avro_limit_select.csv" should exist
