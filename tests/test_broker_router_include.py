



from fastpubsub.broker import PubSubBroker
from fastpubsub.router import PubSubRouter


def test_router_inclusion_on_constructor():
    grandchild_router = PubSubRouter("grandchild")
    child_router = PubSubRouter("child", routers=(grandchild_router,))
    parent_router = PubSubRouter("parent", routers=(child_router,))
    another_parent_router = PubSubRouter("another_parent")

    broker = PubSubBroker(project_id="some_project_id", routers=(parent_router, another_parent_router))

    assert len(another_parent_router.routers) == 0
    assert len(grandchild_router.routers) == 0
    assert len(child_router.routers) == 1
    assert len(parent_router.routers) == 1
    assert len(broker.router.routers) == 2

    routers = (grandchild_router, child_router, parent_router, another_parent_router)
    for router in routers:
        assert broker.router.project_id == router.project_id

    assert another_parent_router.prefix == "another_parent"
    assert parent_router.prefix == "parent"
    assert child_router.prefix == "parent.child"
    assert grandchild_router.prefix == "parent.child.grandchild"


def test_router_inclusion_on_include_router():
    grandchild_router = PubSubRouter("grandchild")

    child_router = PubSubRouter("child")
    child_router.include_router(grandchild_router)

    parent_router = PubSubRouter("parent")
    parent_router.include_router(child_router)

    another_parent_router = PubSubRouter("another_parent")

    broker = PubSubBroker(project_id="some_project_id")
    broker.include_router(parent_router)
    broker.include_router(another_parent_router)

    assert len(another_parent_router.routers) == 0
    assert len(grandchild_router.routers) == 0
    assert len(child_router.routers) == 1
    assert len(parent_router.routers) == 1
    assert len(broker.router.routers) == 2

    routers = (grandchild_router, child_router, parent_router, another_parent_router)
    for router in routers:
        assert broker.router.project_id == router.project_id

    assert another_parent_router.prefix == "another_parent"
    assert parent_router.prefix == "parent"
    assert child_router.prefix == "parent.child"
    assert grandchild_router.prefix == "parent.child.grandchild"
