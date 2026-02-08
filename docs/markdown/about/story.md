---
icon: lucide/feather
---

# Story of FastPubSub
_by Sandro Matheus Vila Nova Marques_

I did not wake up one morning and decide to write a framework.

I woke up to alarms.

At the beginning of 2025, we ran a system that looked clean on architecture diagrams: a webhook received financial data, we cleaned it, published to Google Cloud Pub/Sub, and a fleet of workers consumed those messages to drive partner integrations downstream. It was simple enough to explain. It was complex enough to fail.

Then a partner sent us a payload that passed the “looks fine” test. It even behaved like a normal message-until it reached the wrong place. In a worker it turned into a poison pill, and the incident taught us something ruthless: message delivery is not the same as system safety. Pub/Sub did what Pub/Sub does.

The message failed and came back...

It failed and came back...

It failed and came back!!!

We had weak log correlation, retries were eager, and our subscriptions weren’t forcing a dead-letter path by default, so failure had nowhere to land except back in the loop. Also because the Python Pub/Sub SDK is threading-first while our services were asyncio-first, we hit a particularly sharp edge under pressure. In one consumer, the failure mode multiplied into resource saturation: too many concurrent executions, too many database connections, too many event loops created when the system should have been slowing down.

We stabilized the incident the way you do in the real world: with quick patches, toggles, and workarounds you promise to delete later. But the part that stayed with me was what came after. The same pattern existed across multiple services. This wasn’t “one team did something wrong”. This was a systemic foot-gun that kept finding new feet.

I had one advantage though, my team had excellent engineers (I'm talking to you [Helder](https://www.linkedin.com/in/helderdoutel), [Murilo P.](https://www.linkedin.com/in/murilofpontes) and [Murilo N.](https://www.linkedin.com/in/murilo-nieri)). So I sat down with them to figure out a way of not letting the fire spread. They helped me the way great engineers help: they questioned everything with care and with steel. They were constructive and ruthless in the right way. Every time I tried to hand-wave a detail, they pulled it back into reality. “What happens when it fails?” “What will a developer do at 2 a.m.?” “What’s the safe default?” That critique wasn’t friction. It was shape.

Our first prototype was not “a framework.” It was an attempt to make the safe path the easy path.

We started with a topic-first model: a `PubSubTopic` object that you attach to an app, and then you register subscriber functions on it. We called it **StarConsumers** because we inherited [Starlette](https://starlette.dev/) application object, which gave us lifecycle control, configuration, and a place to hang cross-cutting concerns.

Here is what that first version felt like:

```python
from starconsumers import PubSubTopic, StarConsumers, Message

topic_conn = PubSubTopic(project_id="project_id", topic_name="topic_name")
app = StarConsumers()
app.attach_topic(topic_conn)


@topic_conn.subscriber("my_subscription")
async def handler(message: Message):
    print("Hello, World")
```

From there we wrote requirements the way production writes requirements:

- Correlated logs and context propagation, because a message without a trace is a ghost.
- Instrumentation hooks (New Relic / OpenTelemetry), because “it’s slow” is not a diagnosis.
- Sensible defaults, because Pub/Sub’s defaults are not always the defaults you want.
- Backoff retries and dead-letter routing, because “try again” needs a plan.
- Health checks and probes, because “alive” and “consuming” are different truths.
- A framework-managed asyncio lifecycle, so event loops stop being a hidden tax.
- A CLI to orchestrate consumers like teams actually run them (“A and B, not C”).
- Subscription filters and Pub/Sub-specific knobs, surfaced in a way developers will actually use.
- A registration style that feels like [FastAPI](https://fastapi.tiangolo.com/): write the function, decorate it, and keep the plumbing out of the business logic.

By around June 2025, we had something working. By September/October 2025, we had something “okay.” It ran. It shipped. And still, the structure bothered me. It was too tightly coupled to FastAPI in the wrong places, and I could feel the maintenance debt forming in the joints.

So we went looking for patterns that already worked. The project that influenced a lot of our thinking was [FastStream](https://faststream.ag2.ai/). It had already built an opinionated, developer-friendly way to implement stream consumers and producers for systems like Redis. It proved the idea: you can make message-driven code feel ergonomic and disciplined at the same time.

We considered contributing Pub/Sub support upstream. That would have been a clean story: add the missing piece, send the PR, share maintenance. But Pub/Sub is not “just another broker.” We needed Pub/Sub-specific configuration and a CLI-first orchestration model. The fit wasn’t right, and forcing it would have made both systems worse.

So we did the harder thing: we built the focused tool we actually needed.

We almost kept the name **StarConsumers**, but the project was no longer Starlette-first. It was shaped by FastAPI’s developer experience, by FastStream’s philosophy of making streaming code feel natural, and by the reality of Pub/Sub in production. The name that stuck was the one that told the truth plainly: **FastPubSub**.

FastPubSub exists because of a struggle most developers recognize: the day your system behaves exactly as designed, and you realize the design lets you fail too easily. My hope is simple. If you build message-driven systems, I want you to feel less alone when the alarms sing. I want you to ship safer defaults, demand better observability, and treat developer experience as a reliability feature.

In the end, you are only reading this because I never wanted to fight that same fire twice.
