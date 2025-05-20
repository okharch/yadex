# ⚡ yadex — Yet Another Data EXchange

A **lightweight, real-time + historical MongoDB sync** engine written in Go.  
Originally built as a proof-of-concept after 6 years of overengineering by others — completed in just **1 month** with clarity and purpose.

> ✅ Simple — 💨 Fast — 🧱 Robust — 🧠 Smart

---

## ✨ Why It Exists

This repository showcases **how things should be implemented**:  
Without bloated queue managers, without unnecessary RabbitMQ hops, and without overcomplicating the architecture.  

Despite my preference for **relational databases**, I’ve worked extensively with MongoDB.  
This project demonstrates:
- My ability to **learn and master unfamiliar tech quickly**
- A **production-grade solution** using the **MongoDB oplog**
- A clear replacement for fragile legacy sync solutions

---

## 🔁 Sync Logic Overview

MongoDB data is split into two categories:

- 🟢 **RT (Real-Time) Data** — recent, high-frequency updates
- 🔵 **ST (Stable/Historical) Data** — bulk historical records

Each class follows its own syncing strategy:

### 🟢 Real-Time (RT) Sync
- 📰 Listens to MongoDB [oplog](https://www.mongodb.com/docs/manual/core/replica-set-oplog/)
- 📦 Collects changes into a bulk (up to `MAX_RT_BULK_SIZE` or after `RT_DELAY`)
- 🚫 If flushing to the receiver fails — the changes are dropped (stateless & efficient)
- ⏳ Expired data is ignored

### 🔵 Historical (ST) Sync
- 🔍 First checks whether a collection has already been cloned
- 📦 If not, it clones all documents from sender → receiver
- 🧠 After cloning, follows oplog to track future changes
- 🕐 Keeps bookmarks for resumable sync
- 🧹 If bookmarks are missing or expired, performs unordered insert of all documents

---

## ⚙️ Features

- 🧾 YAML-based config defines RT vs ST collections
- 🔄 Drop & resume anytime (as long as oplog isn’t capped)
- 📉 Gracefully degrades if oplog is unavailable
- 🔐 Assumes minimal conflicts on the receiver

---

## 📦 Repo Highlights

- ✅ Clean architecture with minimal dependencies
- 🧪 Easy to understand & extend
- 🔥 Proven in a real-world scenario that others failed to solve

---

## 🧠 TL;DR

> “Keep it simple. Focus on correctness. Avoid Rabbit holes 🐇.”

For details, see the source code and config examples.

---

Made with ☕ and ❤️ by [okharch](https://github.com/okharch)
