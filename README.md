# Парсер пользовательских логов системы КонсультантПлюс

Проект для обработки и анализа логов пользовательских сессий системы КонсультантПлюс

## 📌 Архитектура проекта

[![Архитектура проекта](https://docs.google.com/drawings/d/e/2PACX-1vRxt3x7fbQ7phT_3z8eUpc5nJW_NbU5yCgOsPuT7kAOCSWxXyZ7oKKLic5spuktzNir2fjPjQ-ZrP73/pub?w=901&h=720)](
https://docs.google.com/drawings/d/e/2PACX-1vRxt3x7fbQ7phT_3z8eUpc5nJW_NbU5yCgOsPuT7kAOCSWxXyZ7oKKLic5spuktzNir2fjPjQ-ZrP73/pub?w=901&h=720)

```
===================================================================================================================

                                      +---------------------------+
                                      |           Main            |
                                      +---------------------------+
                                      | - initLogger              |
                                      | - initSparkSession        |
                                      | - processRawData          |
                                      | - task1 (поиск по docId)  |
                                      | - task2 (агрегация docs)  |
                                      | - printResults            |
                                      +---------------------------+
                                                │
   ┌────────────────────────────────────────────┴────────────────────────────────────────────┐
   ▼                                                                                         ▼
+------------------------------------+                                        +--------------------------------+
|  SparkSession                      |                                        |  log4j2 logger (log4j2.xml)   |
|  + wholeTextFiles(inputPath)       |                                        |  + ParseError                 |
|  + errorAccumulator                |                                        |  + logs/errors.log            |
+------------------------------------+                                        +--------------------------------+
        │
        ▼
+---------------------------------------------------------+
|             SessionParser (per file)                    |
|---------------------------------------------------------|
| .parse(content, filePath, errorAcc)                     |
|  ├─ "SESSION_START <date>"  → parseStart()              |
|  ├─ "SESSION_END <date>"    → parseEnd()                |
|  ├─ "QS {...}"              → QuickSearch.parse         |
|  ├─ "CARD_SEARCH_START..."  → CardSearch.parse          |
|  ├─ "DOC_OPEN..."           → DocOpen.parse             |
|  └─ Accumulates errors      → errorAccumulator, logs    |
+------------------┬--------------------------------------+
                   ▼
+---------------------------------------------------------+
|             SessionBuilder                              |
|---------------------------------------------------------|
|  Fields:                                                |
|    - startDate: DateTime                                |
|    - endDate: DateTime                                  |
|    - quickSearches: List[QuickSearch] (mutable)         |
|    - cardSearches:  List[CardSearch] (mutable)          |
|    - docOpens:      List[DocOpen] (mutable)             |
|                                                         |
|  Методы:                                                |
|    - build(): Session                                   |
|    - buildWithRecoveredIds(): Session (через RecoverID) |
+------------------┬--------------------------------------+
                   ▼
+---------------------------------------------------------+
|             RecoverID                                   |
|---------------------------------------------------------|
| Step 1: Восстановление docOpen.id                       |
|   - если docId встречается только в одном QS/CS         |
| Step 2: Восстановление id у QuickSearch и CardSearch    |
|   - если есть docOpen с этим docId и ненулевым id       |
| - Обработка неоднозначных случаев                       |
+------------------┬--------------------------------------+
                   ▼
+---------------------------------------------------------+
|                    Session                              |
|---------------------------------------------------------|
| - startDate: DateTime                                   |
| - endDate: DateTime                                     |
| - quickSearches: List[QuickSearch]                      |
| - cardSearches: List[CardSearch]                        |
| - docOpens: List[DocOpen]                               |
+---------------------------------------------------------+

Сущности событий (Events)
===================================================================================================================

QuickSearch:
------------
| Поля: date, id, query, docIds         |
| Методы:                               |
|  - parse(lines, file, acc, date)      |
|  - extract(content, sessionDate)      |
| Зависит от: DateTime, ParseError, logger

CardSearch:
-----------
| Поля: date, id, query, docIds         |
| Методы:                               |
|  - parse(lines, file, acc, date)      |
|  - extract(lines, sessionDate)        |
| Зависит от: DateTime, ParseError, logger

DocOpen:
--------
| Поля: date, id, docId                 |
| Методы:                               |
|  - parse(lines, file, acc, date)      |
|  - extract(line, sessionDate)         |
| Зависит от: DateTime, ParseError, logger

DateTime:
---------
| parse(str)                            |
| toString()                            |
| Обработка форматов:                   |
|  - "dd.MM.yyyy_HH:mm:ss"              |
|  - "EEE,_dd_MMM_yyyy_HH:mm:ss_+ZZZZ"  |

Тесты
===================================================================================================================

• QuickSearchTest
  - валидный парсинг, спец. символы, пустой запрос
  - некорректные данные

• CardSearchTest
  - корректный блок, падение по дате, неверный формат

• DocOpenTest
  - отсутствие ID, отрицательные ID, некорректный ввод

• RecoverIDTest
  - ID docOpen ← QS/CS
  - QS/CS ← docOpen
  - неоднозначности

• DateTimeTest
  - парсинг, ошибки, форматирование

Задачи (Main)
===================================================================================================================

• task1(docId: String): Long
  - Ищет вхождение docId в CardSearch.query
  - Учитывает кириллические и латинские аналоги букв (регулярка)

• task2(): Seq[(String, String, Int)]
  - Подсчитывает, сколько раз каждый docId из QS реально был открыт


Вывод
===================================================================================================================
• Консоль:
  - Сколько раз производился поиск ACC_45616 через карточку 
  - Топ-5 документов
• CSV:
  - Формат: `date,docId,count` → `output/task2.csv`
```

## 📊 Выходные данные

```
Document ACC_45616 was searched in cards 24 times

Date,DocID,Count

------------------
05.03.2020,ACC_45615,14
16.03.2020,ACC_45616,14
21.04.2020,ACC_45615,13
23.09.2020,ACC_45614,12
13.03.2020,ACC_45616,12
```

- **Подсчет количества раз, когда в карточке производили поиск документа с идентификатором ACC_45616**:
    - Вывод в консоль
    - Результат: **24**


- **Подсчет статистики открытий документов по дням**:
    - Результат доступен в файле `task2.csv`

