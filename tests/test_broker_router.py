



from fastpubsub.broker import PubSubBroker
from fastpubsub.router import PubSubRouter


def test_router_inclusion_on_constructor():
    sales_router = PubSubRouter("sales")
    datateam_router = PubSubRouter("data")

    finance_router = PubSubRouter("finance", routers=(sales_router,))
    core_router = PubSubRouter("core", routers=(finance_router, datateam_router,))

    broker = PubSubBroker(project_id="some_project_id", routers=(core_router,))
    routers = (sales_router, datateam_router, finance_router, core_router)
    for router in routers:
        assert router.project_id == "some_project_id"

    assert core_router.prefix == "core"
    assert finance_router.prefix == "core.finance"
    assert datateam_router.prefix == "core.data"
    assert sales_router.prefix == "core.finance.sales"



def test_router_inclusion_on_include_router():
    sales_router = PubSubRouter("sales")
    datateam_router = PubSubRouter("data")

    finance_router = PubSubRouter("finance")
    finance_router.include_router(sales_router)

    core_router = PubSubRouter("core")
    core_router.include_router(finance_router)
    core_router.include_router(datateam_router)

    broker = PubSubBroker(project_id="some_project_id")
    broker.include_router(router=core_router)

    routers = (sales_router, datateam_router, finance_router, core_router)
    for router in routers:
        assert router.project_id == "some_project_id"

    assert core_router.prefix == "core"
    assert finance_router.prefix == "core.finance"
    assert datateam_router.prefix == "core.data"
    assert sales_router.prefix == "core.finance.sales"











# TESTME:
# 1. Adicionar router no construtor ->
# 2. Adicionar router no include router
# 3. Adicionar router no
