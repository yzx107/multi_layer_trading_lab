from multi_layer_trading_lab.adapters.readiness import DataSourceReadiness
from multi_layer_trading_lab.adapters.source_status import build_source_adapter_status


def test_source_adapter_status_blocks_stub_adapter_even_with_credentials():
    status = build_source_adapter_status(
        DataSourceReadiness(source="tushare", ready=True),
        adapter_status="stub_adapter",
    )

    assert status.credential_ready
    assert not status.live_data_ready
    assert "tushare_stub_adapter" in status.failed_reasons


def test_source_adapter_status_allows_real_adapter_with_credentials():
    status = build_source_adapter_status(
        DataSourceReadiness(source="ifind", ready=True),
        adapter_status="real_adapter",
    )

    assert status.live_data_ready
    assert status.failed_reasons == ()


def test_source_adapter_status_allows_real_file_adapter_with_credentials():
    status = build_source_adapter_status(
        DataSourceReadiness(source="ifind", ready=True),
        adapter_status="real_file_adapter",
    )

    assert status.live_data_ready
    assert status.failed_reasons == ()
