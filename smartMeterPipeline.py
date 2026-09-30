# Dataflow job for the MS3 design part.
# Stages:
#   1. Read from PubSub:  read the smart meter readings (topic: smartMeterReadingsDesign)
#   2. Filter:            drop records with missing measurements (None)
#   3. Convert:           P(psi) = P(kPa) / 6.895,  T(F) = T(C) * 1.8 + 32
#   4. Write to PubSub:   send the converted readings to another topic (smartMeterConverted)
