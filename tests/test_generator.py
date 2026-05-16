from generator.hmis_generator import (DESTINATIONS, LIVING_SITUATIONS,
                                      make_client, make_enrollment,
                                      make_project, make_service)


def test_client_shape():
    c = make_client(1)
    assert c["ClientID"].startswith("C")
    assert c["VeteranStatus"] in (0, 1)


def test_enrollment_keys():
    c = make_client(1); p = make_project(1)
    e = make_enrollment(c, p, 1)
    assert e["PersonalID"] == c["ClientID"]
    assert e["ProjectID"]  == p["ProjectID"]
    if e["Destination"]:
        assert e["Destination"] in DESTINATIONS
    assert e["LivingSituation"] in LIVING_SITUATIONS


def test_service_links_to_enrollment():
    c = make_client(1); p = make_project(1)
    e = make_enrollment(c, p, 1)
    s = make_service(e, 1)
    assert s["EnrollmentID"] == e["EnrollmentID"]
    assert s["QuantityOfServices"] >= 1
