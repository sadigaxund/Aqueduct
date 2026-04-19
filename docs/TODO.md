1. Adapt towards frontend > Discuss
2. Update TESTING.md and make it up to date, moreover, handle the ❌s
3. What would be the best way of handling the iterations, that is if an llm couldn't fix the issue on first go, what should the system do? Should it try again with the same blueprint? Or should it try to fix it again with the already fixed blueprint? What kind of automation strategy should we use? having like a max attempts after which it would notify the user about the failure?

    Moreover, should we really let LLM patches let loose on production environments like that, is there a way to introduce a safe mode where it can reliably run and verify the patch, and only if it is safe and correct, it should be applied to the production environment? Some kind of preview run

4. ok so, I have

❯ okay, so as you said implement both, and keep option B as optional and for immutable sources. I dont understand what you mean by "Add
    watermark strategy later when you have Delta or Kafka connectors", Will we implement connectors? but did not we make so that it is possible  
  to directly use Spark's implementation of sources?                                                                                             