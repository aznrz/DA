# DA — Портфолио по аналитике данных

![Power BI](https://img.shields.io/badge/Power%20BI-Дашборды-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Ollama](https://img.shields.io/badge/LLM-Ollama-FF6B35?style=flat-square)
![Статус](https://img.shields.io/badge/Статус-Активный-2ea44f?style=flat-square)

Этот репозиторий — портфолио реальных аналитических проектов. Здесь собраны интерактивные Power BI дашборды, data pipelines на Python, AI-ассистент с локальной LLM и SQL-аналитика.

Каждая папка — отдельный законченный проект с полным циклом: от сбора и обработки данных до визуализации, инсайтов и выводов.

---

## 📁 Структура репозитория

| Папка | Проект | Инструменты |
|---|---|---|
| `PowerBI/HeadHunterAnalytics` | Анализ рынка вакансий аналитиков | Power BI, Python, HH API |
| `PowerBI/HR_Analytics` | HR-дашборд: текучесть персонала | Power BI, DAX, Excel |
| `PowerBI/ManagersSalesPlan` | KPI менеджеров по продажам | Power BI, SQL, Power Automate |
| `AI/TelegramBOT_7B_Q4` | AI-ассистент в Telegram | Python, Ollama, Mistral 7B |
| `SQL` | Аналитические SQL-запросы | PostgreSQL |

---

## 📊 Power BI

### HeadHunter Analytics
Аналитический дашборд рынка вакансий для аналитиков данных на основе данных HH API.

**Что внутри:**
- ETL-пайплайн на Python: сбор → фильтрация → инкрементальная загрузка
- Анализ зарплат, навыков, географии и динамики вакансий
- Star Schema: FactVacancies + DimDate + DimCompany + DimLocation

**Ключевые инсайты:**

| Показатель | Значение |
|---|---|
| Топ-навык | Excel — 46% вакансий |
| Второй навык | SQL — 36% вакансий |
| Основной город спроса | Алматы — 63% |
| Зарплата (6+ лет) | ~700 000 KZT |
| Высокооплачиваемые навыки | A/B testing, Airflow, ETL |

![Main Page](PowerBI/HeadHunterAnalytics/images/HR%20Analytics%20main%20page.png)

---

### HR Analytics Dashboard
Интерактивный дашборд для анализа текучести персонала и HR-метрик.

**Что внутри:**
- Метрики: % текучести, средний возраст, средняя зарплата, средний стаж
- Анализ по возрасту, должности, образованию, уровню зарплат
- Группы риска и факторы увольнений

**Ключевые инсайты:**
- Наибольшая текучесть — сотрудники с небольшим стажем (первые годы работы)
- Возрастная группа 26–35 наиболее мобильна
- Прямая корреляция: удовлетворённость → удержание

![HR Dashboard](PowerBI/HR_Analytics/image/Main%20Page.png)

---

### Managers Sales Plan
KPI-дашборд для оценки эффективности менеджеров по продажам.

**Что внутри:**
- Сравнение факт vs план по менеджерам, регионам, партнёрам
- Автоматическое обновление данных через Power Automate
- Источники: SQL (PostgreSQL) + SharePoint + Excel

**Стек:**

| Компонент | Инструмент |
|---|---|
| Визуализация | Power BI, DAX |
| Трансформация | Power Query (M) |
| БД | SQL, PostgreSQL |
| Автоматизация | Power Automate |
| Хранение | SharePoint |

---

## 🤖 AI

### AI Mentor Bot
Локальный AI-ассистент для аналитиков данных в Telegram. Работает полностью на вашем железе — никакие данные не уходят в облако.

**Возможности:**
- Помогает с Python (Pandas, NumPy), SQL, Excel-формулами
- Контекстная память диалога, команда `/clear`
- Лаконичные ответы: только суть и готовый код

**Выбор квантования:**

| Уровень | RAM | Рекомендация |
|---|---|---|
| Q3 | < 8 ГБ | Для слабых систем |
| **Q4** | **8–10 ГБ** | **Оптимально (по умолчанию)** |
| Q8 | 12–16 ГБ | Максимальная точность |

---

## 🛠️ Стек технологий

| Область | Инструменты |
|---|---|
| BI & Визуализация | Power BI, DAX, Power Query |
| Языки | Python, SQL |
| ETL & Автоматизация | Power Automate, REST API |
| AI & LLM | Ollama, Mistral 7B, Telegram Bot API |
| Данные | Excel, CSV, SharePoint, HH API |
| Архитектура | Star Schema, Data Modeling |

---

## 📬 Контакты

[![GitHub](https://img.shields.io/badge/GitHub-aznrz-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/aznrz)
