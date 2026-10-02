from feed.delivery import DeliveryTracker, DELIVERED, DROPPED, FILTERED


def test_flush_watermark_excludes_later_out_of_order_delivery():
    tracker = DeliveryTracker()
    first = tracker.begin()
    tracker.persisted(first)
    watermark = tracker.watermark()
    later = tracker.begin()
    tracker.persisted(later)
    tracker.settle([later], DROPPED)
    pending = tracker.wait(watermark, 0)
    assert pending.accepted == pending.persisted_pending == 1
    assert pending.dropped == 0
    tracker.settle([first], DELIVERED)
    report = tracker.wait(watermark, 0)
    assert report.successful and report.delivered == 1
    final = tracker.wait(tracker.watermark(), 0)
    assert final.accepted == final.dropped == 1


def test_long_outage_uses_aggregate_delivery_counts():
    tracker = DeliveryTracker()
    blocked = tracker.begin()
    tracker.persisted(blocked)
    for _ in range(10000):
        ticket = tracker.begin()
        tracker.persisted(ticket)
        tracker.settle([ticket], DELIVERED)
    assert len(tracker._current) <= 5
    report = tracker.wait(tracker.watermark(), 0)
    assert report.delivered == 10000
    assert report.persisted_pending == 1


def test_rejected_and_unsaved_records_have_distinct_counts():
    tracker = DeliveryTracker()
    rejected = tracker.begin()
    tracker.reject(rejected)
    tracker.begin()
    report = tracker.wait(tracker.watermark(), 0)
    assert report.accepted == report.unsaved == report.pending == 1
    assert report.persisted_pending == 0


def test_session_totals_include_completed_flushes_and_pending_records():
    tracker = DeliveryTracker()
    for outcome in (DELIVERED, FILTERED, DROPPED):
        ticket = tracker.begin()
        tracker.persisted(ticket)
        tracker.settle([ticket], outcome)
        assert tracker.wait(tracker.watermark(), 0).accepted == 1
    rejected = tracker.begin()
    tracker.reject(rejected)
    saved = tracker.begin()
    tracker.persisted(saved)
    watermark = tracker.watermark()
    tracker.begin()

    total = tracker.snapshot()
    assert total == tracker.snapshot()
    assert total.accepted == 5
    assert total.delivered == total.filtered == total.failed == 1
    assert total.pending == 2
    assert total.persisted_pending == total.unsaved == 1
    assert not total.successful
    assert tracker.wait(watermark, 0).accepted == 1
