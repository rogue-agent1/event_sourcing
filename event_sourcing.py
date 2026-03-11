#!/usr/bin/env python3
"""Event Sourcing — event store with projections and snapshots."""
import json, time, copy

class Event:
    def __init__(self, aggregate_id, event_type, data, version=0):
        self.aggregate_id = aggregate_id; self.event_type = event_type
        self.data = data; self.version = version; self.timestamp = time.time()
    def __repr__(self): return f"Event({self.event_type}, v{self.version})"

class EventStore:
    def __init__(self):
        self.events = {}; self.snapshots = {}
    def append(self, event):
        self.events.setdefault(event.aggregate_id, []).append(event)
    def get_events(self, aggregate_id, after_version=0):
        return [e for e in self.events.get(aggregate_id, []) if e.version > after_version]
    def save_snapshot(self, aggregate_id, state, version):
        self.snapshots[aggregate_id] = (copy.deepcopy(state), version)
    def get_snapshot(self, aggregate_id):
        return self.snapshots.get(aggregate_id, (None, 0))

class BankAccount:
    def __init__(self, account_id):
        self.id = account_id; self.balance = 0; self.version = 0
        self.pending_events = []
    def deposit(self, amount):
        self.version += 1
        self.pending_events.append(Event(self.id, "Deposited", {"amount": amount}, self.version))
        self.balance += amount
    def withdraw(self, amount):
        if amount > self.balance: raise ValueError("Insufficient funds")
        self.version += 1
        self.pending_events.append(Event(self.id, "Withdrawn", {"amount": amount}, self.version))
        self.balance -= amount
    def apply_event(self, event):
        if event.event_type == "Deposited": self.balance += event.data["amount"]
        elif event.event_type == "Withdrawn": self.balance -= event.data["amount"]
        self.version = event.version
    @classmethod
    def load(cls, store, account_id):
        acc = cls(account_id)
        state, version = store.get_snapshot(account_id)
        if state: acc.balance = state['balance']; acc.version = version
        for event in store.get_events(account_id, version): acc.apply_event(event)
        return acc

if __name__ == "__main__":
    store = EventStore()
    acc = BankAccount("ACC-001")
    acc.deposit(1000); acc.withdraw(200); acc.deposit(500)
    for e in acc.pending_events: store.append(e)
    print(f"Balance: {acc.balance}, Version: {acc.version}")
    store.save_snapshot(acc.id, {'balance': acc.balance}, acc.version)
    # Reload from events
    acc2 = BankAccount.load(store, "ACC-001")
    print(f"Loaded balance: {acc2.balance}, version: {acc2.version}")
