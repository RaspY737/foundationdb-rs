use foundationdb::{metrics::TransactionMetrics, *};
mod common;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use futures::future;
use futures::prelude::*;

const KEY: &[u8] = b"test_txn_async";
const KEY_NO_EXIST: &[u8] = b"test_txn_async_not_exist";
const VALUE: &[u8] = b"value";

#[test]
fn test_metrics() {
    let _guard = unsafe { foundationdb::boot() };
    futures::executor::block_on(test_txn_async()).expect("failed to run");
    futures::executor::block_on(test_txn_async_get_range()).expect("failed to run");
    futures::executor::block_on(instrumented_run_success()).expect("failed to run");
    futures::executor::block_on(instrumented_run_with_n_retries()).expect("failed to run");
    futures::executor::block_on(test_transaction_info()).expect("failed to run");
    futures::executor::block_on(test_metrics_structure()).expect("failed to run");
    futures::executor::block_on(test_time_metrics()).expect("failed to run");
    futures::executor::block_on(test_custom_metrics()).expect("failed to run");
    futures::executor::block_on(test_transaction_custom_metrics()).expect("failed to run");
}

async fn test_txn_async() -> FdbResult<()> {
    let db = common::database().await?;
    let metrics = TransactionMetrics::new();

    {
        // DESCRIPTION ------------------------------
        // Check the handle metrics with a SET on FDB
        // ------------------------------------------

        // SET TRANSACTION --------------------------
        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");
        txn.set(KEY, VALUE);
        // ------------------------------------------

        // CHECKING... ------------------------------
        let metrics_data = metrics.get_metrics_data();
        let report = metrics_data.total;

        let expected_value = 1;
        assert_eq!(report.call_set, expected_value);

        let expected_value = (KEY.len() + VALUE.len()) as u64;
        assert_eq!(report.bytes_written, expected_value);
        // ------------------------------------------

        txn.commit().await.expect("Could not commit");
    }

    {
        // DESCRIPTION ------------------------------
        // Check the handle metrics with a GET on FDB
        // ------------------------------------------

        // GET TRANSACTION --------------------------
        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");
        let value = txn.get(KEY, false).await?.unwrap();
        let not_found = txn.get(KEY_NO_EXIST, false).await?;
        // ------------------------------------------

        // CHECKING... ------------------------------
        {
            assert_eq!(*value, *VALUE);
            assert!(not_found.is_none());
        }

        {
            let metrics_data = metrics.get_metrics_data();
            let report = metrics_data.total;

            let expected_value = 2;
            assert_eq!(report.call_get, expected_value);

            let expected_value = (KEY.len() + VALUE.len() + KEY_NO_EXIST.len()) as u64;
            assert_eq!(report.bytes_read, expected_value);
        }
        // ------------------------------------------
    }

    Ok(())
}

async fn test_txn_async_get_range() -> FdbResult<()> {
    let db = common::database().await?;
    let metrics = TransactionMetrics::new();

    const N: usize = 100000;

    {
        // DESCRIPTION ------------------------------
        // Check the metrics with a GET RANGE
        // ------------------------------------------

        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");

        let key_begin = "test-range-metrics-";
        let key_end = "test-range-metrics.";

        let mut expected_bytes = 0;

        // SET N KEYS-VALUES ------------------------
        for i in 0..N {
            let key = format!("{}-{}", key_begin, i);
            let value = "toto";
            txn.set(key.as_bytes(), value.as_bytes());

            expected_bytes += (key.len() + value.len()) as u64
        }
        // ------------------------------------------
        // GET RANGE --------------------------------
        let begin = KeySelector::first_greater_or_equal(Cow::Borrowed(key_begin.as_bytes()));
        let end = KeySelector::first_greater_than(Cow::Borrowed(key_end.as_bytes()));
        let opt = RangeOption::from((begin, end));
        let count = txn
            .get_ranges(opt, false)
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?;
        // ------------------------------------------

        // CHECKING... ------------------------------
        assert_eq!(count, N);

        let metrics_data = metrics.get_metrics_data();
        let report = metrics_data.total;

        assert_eq!(report.call_set, N as u64);

        assert_eq!(report.bytes_read, expected_bytes);
        assert_eq!(report.bytes_written, expected_bytes);
        // ------------------------------------------
    }

    Ok(())
}

async fn instrumented_run_success() -> FdbResult<()> {
    let db = common::database().await?;

    let (result, metrics) = match db
        .instrumented_run(|txn, _| async move {
            txn.set(b"foo", b"bar");
            Ok(42)
        })
        .await
    {
        Ok((result, metrics)) => (result, metrics),
        Err(_err) => {
            panic!()
        }
    };

    assert_eq!(result, 42);
    let total = metrics.total;
    let transaction_info = metrics.transaction;
    assert_eq!(total.call_set, 1);
    assert_eq!(total.bytes_written, 6);
    assert_eq!(transaction_info.retries, 0);

    Ok(())
}

async fn instrumented_run_with_n_retries() -> FdbResult<()> {
    let db = common::database().await?;

    // Number of retries we want to force
    const EXPECTED_RETRIES: u64 = 3;

    // Use Arc<Mutex<>> to share and modify the counter across async calls
    let attempt_counter = Arc::new(Mutex::new(0));

    let (result, metrics) = match db
        .instrumented_run(|txn, _| {
            let counter = attempt_counter.clone();
            async move {
                // Set a key to verify metrics
                txn.set(b"foo", b"bar");

                // Increment the counter and check if we should still fail
                let mut attempts = counter.lock().unwrap();
                *attempts += 1;

                if *attempts <= EXPECTED_RETRIES {
                    // Return a retryable error (not_committed) for the first N attempts
                    let fdb_error = FdbError::from_code(1020);
                    // Return FdbBindingError directly - instrumented_run will handle metrics
                    Err(FdbBindingError::from(fdb_error))
                } else {
                    // Succeed on attempt N+1
                    Ok(42)
                }
            }
        })
        .await
    {
        Ok((result, metrics)) => (result, metrics),
        Err(err) => {
            panic!("Test failed: {:?}", err);
        }
    };

    // Verify the result
    assert_eq!(result, 42);

    // Verify the metrics
    let total = metrics.total;
    let transaction_info = metrics.transaction;
    assert_eq!(total.call_set, EXPECTED_RETRIES + 1); // One set operation per attempt
    assert_eq!(total.bytes_written, (EXPECTED_RETRIES + 1) * 6); // 6 bytes per set
    assert_eq!(transaction_info.retries, EXPECTED_RETRIES); // Should match our expected retry count

    // Verify the counter
    let final_attempts = *attempt_counter.lock().unwrap();
    assert_eq!(final_attempts, EXPECTED_RETRIES + 1); // Total attempts should be retries + 1

    Ok(())
}

/// Test for all TransactionInfo fields in MetricsData
async fn test_transaction_info() -> FdbResult<()> {
    let db = common::database().await?;

    // Test read_version field
    {
        // Create a transaction with metrics
        let metrics = TransactionMetrics::new();
        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");

        // Get the read version from the transaction
        let read_version = txn.get_read_version().await?;

        // Set the read version in metrics
        metrics.set_read_version(read_version);

        // Verify the read version was set correctly
        let transaction_info = metrics.get_transaction_info();
        assert_eq!(transaction_info.read_version, Some(read_version));

        // Commit the transaction
        txn.commit().await?;
    }

    // Test commit_version field
    {
        // Test with a write transaction
        let _metrics = TransactionMetrics::new();

        let (_result, metrics_data) = db
            .instrumented_run(|txn, _| async move {
                // Perform a write operation
                txn.set(b"test_commit_version", b"value");
                Ok(())
            })
            .await
            .expect("Transaction failed");

        // Verify transaction info
        let transaction_info = metrics_data.transaction;

        // Commit version should be set (we don't know the exact value)
        assert!(transaction_info.commit_version.is_some());
    }

    // Test retries field
    {
        // Number of retries we want to force
        const EXPECTED_RETRIES: u64 = 2;

        // Use Arc<Mutex<>> to share and modify the counter across async calls
        let attempt_counter = Arc::new(Mutex::new(0));

        // Run a transaction that will be retried
        let result = db
            .instrumented_run(|_txn, _| {
                let counter = attempt_counter.clone();
                async move {
                    // Increment the counter and check if we should still fail
                    let mut attempts = counter.lock().unwrap();
                    *attempts += 1;

                    if *attempts <= EXPECTED_RETRIES {
                        // Return a retryable error for the first N attempts
                        let fdb_error = FdbError::from_code(1020); // not_committed
                        Err(FdbBindingError::from(fdb_error))
                    } else {
                        // Succeed on attempt N+1
                        Ok(())
                    }
                }
            })
            .await;

        match result {
            Ok((_, metrics_data)) => {
                // Verify retry count
                let transaction_info = metrics_data.transaction;
                assert_eq!(transaction_info.retries, EXPECTED_RETRIES);
            }
            Err(_) => panic!("Transaction should have succeeded after retries"),
        }
    }

    Ok(())
}

async fn test_metrics_structure() -> FdbResult<()> {
    let db = common::database().await?;

    // Test current and total metrics
    {
        // Create metrics and transaction
        let metrics = TransactionMetrics::new();
        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");

        // Perform different operations to test various metrics
        txn.set(b"test_metrics_key1", b"value1");
        txn.set(b"test_metrics_key2", b"value2");
        txn.clear(b"test_metrics_key3");

        // Get a value to test read metrics
        let _ = txn.get(b"test_metrics_key1", false).await?;

        // Get metrics data
        let metrics_data = metrics.get_metrics_data();

        // Test current metrics
        let current = metrics_data.current;
        assert_eq!(current.call_set, 2);
        assert_eq!(current.call_clear, 1);
        assert_eq!(current.call_get, 1);
        assert!(current.bytes_written > 0);

        // Test total metrics (should match current since no retries)
        let total = metrics_data.total;
        assert_eq!(total.call_set, 2);
        assert_eq!(total.call_clear, 1);
        assert_eq!(total.call_get, 1);
        assert!(total.bytes_written > 0);

        // Test reset_current
        metrics.reset_current();
        let metrics_data = metrics.get_metrics_data();
        let current_after_reset = metrics_data.current;

        // Current metrics should be reset
        assert_eq!(current_after_reset.call_set, 0);
        assert_eq!(current_after_reset.call_clear, 0);
        assert_eq!(current_after_reset.call_get, 0);

        // Total metrics should remain unchanged
        let total_after_reset = metrics_data.total;
        assert_eq!(total_after_reset.call_set, 2);
        assert_eq!(total_after_reset.call_clear, 1);
        assert_eq!(total_after_reset.call_get, 1);

        txn.commit().await.map(|_| ())?;
    }

    Ok(())
}

async fn test_time_metrics() -> FdbResult<()> {
    let db = common::database().await?;

    // Test time metrics
    {
        // Run a transaction to generate time metrics
        let (_result, metrics_data) = db
            .instrumented_run(|txn, _| async move {
                // Perform multiple operations to ensure measurable time
                for i in 0..10 {
                    let key = format!("test_time_metrics_{}", i).into_bytes();
                    txn.set(&key, b"value");
                }
                // Read some data to add more execution time
                let _ = txn.get(b"test_time_metrics_0", false).await?;
                Ok(())
            })
            .await
            .expect("Transaction failed");

        // Get time metrics
        let time_metrics = metrics_data.time;

        // Verify time metrics
        assert!(
            time_metrics.total_execution_ms > 0,
            "Total execution time should be recorded"
        );
        assert!(
            time_metrics.commit_execution_ms > 0,
            "Commit execution time should be recorded"
        );

        // Run a transaction that will generate error handling time
        let attempt_counter = Arc::new(Mutex::new(0));
        let result = db
            .instrumented_run(|_txn, _| {
                let counter = attempt_counter.clone();
                async move {
                    let mut attempts = counter.lock().unwrap();
                    *attempts += 1;

                    if *attempts == 1 {
                        // Return a retryable error on first attempt
                        let fdb_error = FdbError::from_code(1020); // not_committed
                        Err(FdbBindingError::from(fdb_error))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        if let Ok((_, metrics_data)) = result {
            let time_metrics = metrics_data.time;

            // Verify error handling time
            assert!(
                !time_metrics.on_error_execution_ms.is_empty(),
                "Error handling time should be recorded"
            );
            assert!(
                time_metrics.get_total_error_time() > 0,
                "Total error time should be greater than zero"
            );
        } else {
            panic!("Transaction should have succeeded after retry");
        }
    }

    Ok(())
}

async fn test_custom_metrics() -> FdbResult<()> {
    let db = common::database().await?;

    // Test custom metrics
    {
        // Create metrics and transaction
        let metrics = TransactionMetrics::new();
        let txn = db
            .create_instrumented_trx(metrics.clone())
            .expect("Could not create transaction");

        // Test custom metrics with labels
        metrics.set_custom(
            "custom_counter",
            123,
            &[("tenant", "test"), ("region", "us-west")],
        );
        metrics.set_custom("custom_timer", 456, &[("operation", "write")]);

        // Test incrementing custom metrics
        metrics.increment_custom("incremented_counter", 5, &[("service", "api")]);
        metrics.increment_custom("incremented_counter", 10, &[("service", "api")]);

        // Get metrics data
        let metrics_data = metrics.get_metrics_data();
        let custom = metrics_data.custom_metrics;

        // Verify custom counter with labels
        let key = foundationdb::metrics::MetricKey::new(
            "custom_counter",
            &[("tenant", "test"), ("region", "us-west")],
        );
        let custom_counter = custom.get(&key).copied();
        assert_eq!(custom_counter, Some(123));

        // Verify custom timer with labels
        let key = foundationdb::metrics::MetricKey::new("custom_timer", &[("operation", "write")]);
        let custom_timer = custom.get(&key).copied();
        assert_eq!(custom_timer, Some(456));

        // Verify incremented counter
        let key =
            foundationdb::metrics::MetricKey::new("incremented_counter", &[("service", "api")]);
        let incremented = custom.get(&key).copied();
        assert_eq!(incremented, Some(15));

        // Commit the transaction
        txn.commit().await?;
    }

    Ok(())
}

async fn test_transaction_custom_metrics() -> Result<(), FdbBindingError> {
    let db = common::database().await?;

    // Test transaction custom metrics methods using instrumented_run
    let result = db
        .instrumented_run(|txn, _| async move {
            // Set custom metrics directly on the transaction
            txn.set_custom_metric("txn_counter", 100, &[("operation", "read")])?;
            txn.set_custom_metric("txn_timer", 200, &[("component", "storage")])?;

            // Increment a custom metric
            txn.increment_custom_metric("txn_incremented", 10, &[("type", "query")])?;
            txn.increment_custom_metric("txn_incremented", 15, &[("type", "query")])?;

            // Read a value to make sure the transaction does something
            let _value = txn.get(b"test_key", false).await?;

            Ok(())
        })
        .await;

    // Verify the result and check metrics in the returned metrics data
    match result {
        Ok((_, metrics_data)) => {
            let custom = metrics_data.custom_metrics;

            // Verify metrics were properly recorded
            let key =
                foundationdb::metrics::MetricKey::new("txn_counter", &[("operation", "read")]);
            assert_eq!(custom.get(&key).copied(), Some(100));

            let key =
                foundationdb::metrics::MetricKey::new("txn_timer", &[("component", "storage")]);
            assert_eq!(custom.get(&key).copied(), Some(200));

            let key =
                foundationdb::metrics::MetricKey::new("txn_incremented", &[("type", "query")]);
            assert_eq!(custom.get(&key).copied(), Some(25));
        }
        Err((err, _)) => {
            return Err(err);
        }
    }

    Ok(())
}
