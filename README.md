# Парсер пользовательских логов системы КонсультантПлюс

Проект для обработки и анализа логов пользовательских сессий системы КонсультантПлюс

## 📌 Диаграмма тестов

```mermaid
graph TD
    A[Тестирование системы] --> B[тесты]
    
    B --> B1[Тесты парсеров]
    B1 --> B11[CardSearchTest]
    B1 --> B12[DocOpenTest]
    B1 --> B13[QuickSearchTest]
    B1 --> B14[SessionTest]
    B1 --> B15[DateTimeTest]
    
    B --> B2[Тесты процессоров]
    B2 --> B21[MapDocOpensTest]
    B2 --> B22[RecoverEmptyDateTest]
    B2 --> B23[RecoverIDTest]
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

