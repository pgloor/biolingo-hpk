#!/usr/bin/env python3
"""Electrode health at a glance, straight from the server: sat % = raw ADC samples pinned at a rail
(before any filtering), p2p = peak-to-peak of the low-passed signal, mv = mean.
Run on the server:  watch -n 10 python3 sat.py [device-prefix]"""
import json, sys, urllib.request
prefix = sys.argv[1] if len(sys.argv) > 1 else "biolingo-"
d = json.load(urllib.request.urlopen("http://127.0.0.1:8080/api/status"))
for h in d.get("heartbeats", d.get("devices", [])):
    if h["device"].startswith(prefix) and h.get("ago_s", 1e9) < 120:
        print(f'{h["device"]:28s} sat {h.get("sat"):5.1f} %  p2p {h.get("p2p"):7.1f} mV  mean {h.get("mv"):7.1f} mV  rssi {h.get("rssi")}')
