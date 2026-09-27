---
'crossflight': minor
---

Add an opt-in whole-flight deadline - `defaultFlightDeadlineMs` on the factory or `flightDeadlineMs` per call - which aborts the shared flight when the budget passes, so joiners cannot each wait a fresh full timeout and a caller arriving afterwards is rejected instead of starting a new flight.
