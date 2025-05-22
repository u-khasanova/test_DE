# Парсер пользовательских логов системы КонсультантПлюс

Проект для обработки и анализа логов пользовательских сессий системы КонсультантПлюс

## 📌 Архитектура проекта

[![Архитектура проекта](https://docs.google.com/drawings/d/e/2PACX-1vRxt3x7fbQ7phT_3z8eUpc5nJW_NbU5yCgOsPuT7kAOCSWxXyZ7oKKLic5spuktzNir2fjPjQ-ZrP73/pub?w=901&h=720)](
https://docs.google.com/drawings/d/e/2PACX-1vRxt3x7fbQ7phT_3z8eUpc5nJW_NbU5yCgOsPuT7kAOCSWxXyZ7oKKLic5spuktzNir2fjPjQ-ZrP73/pub?w=901&h=720)

```mermaid
flowchart TB
    subgraph Парсеры
        A[CardSearch] --> A1[Парсинг даты]
        A --> A2[Парсинг ID]
        A --> A3[Парсинг запроса]
        B[DocOpen] --> B1[Парсинг даты]
        B --> B2[Парсинг ID]s
        C[QuickSearch] --> C1[Парсинг даты]
        C --> C2[Парсинг ID]
        D[Session] --> D1[Парсинг start/end]
    end
    
    subgraph Процессоры
        E[MapDocOpens] --> E1[Связывание DocOpen]
        F[RecoverEmptyDate] --> F1[Восстановление дат]
        G[RecoverID] --> G1[Восстановление ID]
    end
    
    subgraph Тесты
        T1[CardSearchTest] -->|Проверяет| A
        T2[DocOpenTest] -->|Проверяет| B
        T3[QuickSearchTest] -->|Проверяет| C
        T4[SessionTest] -->|Проверяет| D
        T5[MapDocOpensTest] -->|Проверяет| E
        T6[RecoverEmptyDateTest] -->|Проверяет| F
        T7[RecoverIDTest] -->|Проверяет| G
    end
    
    style Парсеры fill:#f9f2d9,stroke:#333
    style Процессоры fill:#d9f9f9,stroke:#333
    style Тесты fill:#e2f9d9,stroke:#333
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

