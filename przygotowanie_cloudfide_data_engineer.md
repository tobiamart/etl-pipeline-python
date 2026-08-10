# Przygotowanie do rozmowy — Junior Data Engineer, Cloudfide

---

## 1. SQL

### JOIN-y
- **INNER JOIN** — zwraca tylko wiersze, które mają dopasowanie w obu tabelach.
- **LEFT JOIN** — wszystkie wiersze z lewej tabeli + dopasowane z prawej (NULL, jeśli brak dopasowania). Najczęściej używany w data engineeringu, np. gdy chcesz zachować wszystkich klientów, nawet bez zamówień.
- **RIGHT JOIN** — odwrotność LEFT JOIN, rzadziej używany (łatwiej zamienić tabele i użyć LEFT).
- **FULL OUTER JOIN** — wszystkie wiersze z obu tabel, dopasowane gdzie się da, reszta NULL.
- **CROSS JOIN** — iloczyn kartezjański (każdy z każdym), rzadko potrzebny.

Typowe pytanie: "Kiedy JOIN zwróci więcej wierszy niż oczekujesz?" — odpowiedź: gdy klucz łączący nie jest unikalny w jednej z tabel (duplikaty po stronie JOIN-owanej powodują powielenie wierszy).

### WHERE vs HAVING
- `WHERE` filtruje wiersze **przed** grupowaniem (GROUP BY).
- `HAVING` filtruje **grupy** — czyli wyniki agregacji (SUM, COUNT, AVG itd.), już po GROUP BY.
- Przykład: `SELECT dzial, COUNT(*) FROM pracownicy GROUP BY dzial HAVING COUNT(*) > 5` — pokaż działy z więcej niż 5 pracownikami.

### Window functions (funkcje okienkowe)
Bardzo częsty temat na rozmowach data engineering — warto umieć to swobodnie tłumaczyć.

- Działają na "oknie" wierszy powiązanych z bieżącym wierszem, ale **nie redukują liczby wierszy** (w przeciwieństwie do GROUP BY).
- Składnia: `FUNKCJA() OVER (PARTITION BY kolumna ORDER BY kolumna)`

Najważniejsze:
- **ROW_NUMBER()** — nadaje unikalny numer kolejny w obrębie partycji (przydatne np. do usuwania duplikatów — zostawiasz tylko `ROW_NUMBER() = 1`).
- **RANK()** — jak ROW_NUMBER, ale remisy dostają ten sam numer, a kolejny numer jest "przeskakiwany" (1,1,3).
- **DENSE_RANK()** — jak RANK, ale bez przeskakiwania (1,1,2).
- **LAG() / LEAD()** — dostęp do poprzedniego/następnego wiersza w obrębie partycji, super przydatne np. do liczenia różnicy dzień do dnia.

Przykład klasycznego pytania — "znajdź drugą najwyższą pensję":
```sql
SELECT DISTINCT salary
FROM (
  SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rnk
  FROM employees
) t
WHERE rnk = 2;
```

Usuwanie duplikatów przez window function:
```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY email ORDER BY id) as rn
  FROM users
)
DELETE FROM ranked WHERE rn > 1;
```

### Indeksy
- Struktura przyspieszająca wyszukiwanie (jak indeks w książce) — kosztem miejsca i wolniejszych zapisów (INSERT/UPDATE muszą aktualizować też indeks).
- **Clustered index** — fizycznie sortuje dane tabeli wg tego klucza (jedna na tabelę, zwykle na primary key).
- **Non-clustered index** — osobna struktura wskazująca na wiersze, można ich mieć wiele.
- Kiedy stosować: na kolumnach często używanych w WHERE, JOIN, ORDER BY. Nie na kolumnach o niskiej selektywności (np. płeć — mało unikalnych wartości).

### Normalizacja vs denormalizacja
- **Normalizacja** — dzielenie danych na powiązane tabele, żeby uniknąć redundancji (formy normalne 1NF, 2NF, 3NF). Dobra dla systemów transakcyjnych (OLTP) — spójność danych.
- **Denormalizacja** — celowe dublowanie danych, żeby przyspieszyć odczyt (mniej JOIN-ów). Typowe w hurtowniach danych / analityce (OLAP), gdzie liczy się szybkość zapytań, nie oszczędność miejsca.
- W data engineeringu często pracujesz właśnie w kierunku denormalizacji (schematy gwiazdy — star schema) pod raportowanie.

---

## 2. Python pod kątem data engineering

To inny kontekst niż ML — nacisk na przetwarzanie i transformację danych, nie na modelowanie.

### Pandas — must-know
- Wczytywanie/zapisywanie danych: `pd.read_csv()`, `pd.read_json()`, `pd.read_parquet()`, `to_csv()`, `to_sql()`
- Czyszczenie: `dropna()`, `fillna()`, `drop_duplicates()`, zmiana typów (`astype`)
- Transformacje: `groupby()`, `merge()` (odpowiednik JOIN), `pivot_table()`, `apply()`
- Filtrowanie: `df[df['col'] > x]`

### Praca z dużymi plikami (nie mieszczą się w pamięci)
To częste pytanie koncepcyjne — pokazuje, czy rozumiesz ograniczenia:
- **Chunking** — wczytywanie plikiem porcjami: `pd.read_csv(path, chunksize=10000)`
- **Generatory** zamiast list — przetwarzanie strumieniowe, jeden element na raz, bez trzymania wszystkiego w pamięci
- Formaty kolumnowe jak **Parquet** — czytasz tylko potrzebne kolumny, dużo bardziej wydajne niż CSV
- Narzędzia do danych większych niż RAM: **Dask**, **Spark/PySpark**, przetwarzanie w chunkach bezpośrednio w bazie danych (SQL zamiast pandas)

### Podstawy, które mogą wypłynąć
- **Dekoratory** — funkcja, która "owija" inną funkcję, żeby dodać funkcjonalność bez zmiany jej kodu (np. `@staticmethod`, logging, retry logic — retry jest bardzo praktyczny w data engineering przy nieudanych połączeniach z API/bazą).
- **Generatory vs listy** — generator (`yield`) oblicza wartości "leniwie", jedna na raz, oszczędza pamięć; lista trzyma wszystko naraz.
- **Obsługa wyjątków** — `try/except/finally`, ważne przy pracy z zewnętrznymi źródłami danych (API, pliki, bazy — zawsze coś może się nie udać).

---

## 3. Cloud / Azure — koncepcje

Nie musisz być ekspertem Azure, ale powinieneś swobodnie rozróżniać poniższe pojęcia.

### Data Warehouse vs Data Lake vs Data Lakehouse
- **Data Warehouse** — dane ustrukturyzowane, przetworzone, zoptymalizowane pod raportowanie/BI (np. Azure Synapse, Snowflake). Schema-on-write (struktura definiowana przed zapisem).
- **Data Lake** — surowe dane w dowolnym formacie (pliki, logi, JSON, obrazy), tanie przechowywanie, elastyczność. Schema-on-read (strukturę nadajesz przy odczycie).
- **Data Lakehouse** — hybryda: elastyczność data lake + wydajność i zarządzalność warehouse'u (to jest właśnie idea Databricks/Delta Lake — wspomniane w ogłoszeniu).

### ETL vs ELT
- **ETL (Extract, Transform, Load)** — dane transformujesz **przed** załadowaniem do docelowego systemu. Tradycyjne podejście, transformacja dzieje się na osobnym serwerze/silniku.
- **ELT (Extract, Load, Transform)** — najpierw ładujesz surowe dane do docelowego systemu (np. lakehouse), transformację robisz **już tam**, wykorzystując moc obliczeniową samego systemu (np. Databricks, Snowflake). Dominujące podejście w nowoczesnych architekturach cloudowych — bo cloud daje tanią moc obliczeniową na żądanie.

### Kluczowe usługi Azure do danych
- **Azure Data Factory (ADF)** — narzędzie do orkiestracji pipeline'ów ETL/ELT, łączenie się ze źródłami danych, harmonogramowanie.
- **Azure Data Lake Storage (ADLS)** — magazyn na surowe dane.
- **Azure Synapse Analytics** — hurtownia danych + analityka na dużą skalę.
- **Databricks (na Azure: Azure Databricks)** — środowisko do przetwarzania danych na dużą skalę przez Spark/PySpark, bardzo mocno powiązane z konceptem lakehouse.

### Orkiestracja (Airflow — "nice to have" w ogłoszeniu)
- Narzędzie do definiowania i harmonogramowania pipeline'ów jako grafów zależności (DAG — Directed Acyclic Graph). Np. "najpierw pobierz dane, potem je oczyść, potem załaduj do bazy" — Airflow pilnuje kolejności i ponawia nieudane kroki.

### CI/CD (mile widziane)
- Automatyzacja testowania i wdrażania kodu (np. pipeline'ów danych) — Azure DevOps lub GitHub Actions. W data engineeringu chodzi głównie o to, żeby zmiany w pipeline'ach były testowane i wdrażane bez ręcznego klikania.

---

## 4. Jak połączyć swoje dotychczasowe doświadczenie z data engineeringiem

Formalnie nie masz 6 miesięcy komercyjnego doświadczenia w DE, więc warto przygotować narrację łączącą Twoje projekty z tym stanowiskiem:

- **Praca inżynierska (anomaly detection w wideo, temporal autoencoders)** — to w praktyce pipeline danych: pozyskiwanie/przetwarzanie dużych ilości danych wideo, przygotowanie ich pod model, ocena jakości wyników. Podkreśl stronę "danych", nie modelu.
- **Projekt rozpoznawania gestów (PyTorch + OpenCV)** — pokazuje pracę z pipeline'em przetwarzania danych na żywo.
- **Flask + LLM API** — pokazuje umiejętność budowania systemów, integracji z zewnętrznymi API (podobne wyzwania jak przy integracji ze źródłami danych).
- **Kurs Anthropic Academy** — pokazuje samodzielne dokształcanie się, co pasuje do kultury firmy ("curiosity, willingness to learn").

Bądź szczery, że komercyjnego doświadczenia stricte DE nie masz, ale pokaż, że fundamenty (SQL, Python, praca z danymi) masz solidne i szybko się uczysz.

---

## 5. Pytania HR — na co się przygotować

- Dlaczego data engineering (a nie np. ML/computer vision, w które dotychczas inwestowałeś czas)?
- Dlaczego Cloudfide konkretnie?
- Opowiedz o swoim najciekawszym projekcie związanym z danymi.
- Jak radzisz sobie z sytuacją, gdy czegoś nie wiesz / uczysz się nowej technologii pod presją czasu?
- Preferowana forma współpracy (B2B/UZ), dyspozycyjność, oczekiwania finansowe.
- Pytania o pracę zdalną i komunikację w zespole rozproszonym (firma jest remote-first, pracuje z klientami międzynarodowymi).

---

## Co warto jeszcze doszlifować przed rozmową
1. Umiejętność napisania prostego zapytania SQL z window function na kartce/na żywo.
2. Krótkie, płynne wytłumaczenie ETL vs ELT i data lake vs warehouse — to niemal na pewno padnie.
3. Przygotowana 2-3 zdaniowa odpowiedź na pytanie o brak formalnego doświadczenia komercyjnego w DE.
