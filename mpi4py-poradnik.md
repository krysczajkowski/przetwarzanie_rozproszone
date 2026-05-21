# mpi4py — Kompletny poradnik do projektu "Taras widokowy"

## 1. Instalacja

### Wymagania
- Python 3.x
- Implementacja MPI zainstalowana w systemie (OpenMPI lub MPICH)

### macOS (Homebrew)
```bash
brew install open-mpi
pip install mpi4py
```

### Linux (Ubuntu/Debian)
```bash
sudo apt install python3-mpi4py
```

### Linux (Fedora/RHEL)
```bash
sudo dnf install python3-mpi4py-mpich
```

### Pip (dowolny system — wymaga zainstalowanego MPI)
```bash
pip install mpi4py
```

### Pip + MPI w jednym kroku
```bash
pip install mpi4py mpich      # lub:
pip install mpi4py openmpi
```

### Weryfikacja
```bash
python -c "from mpi4py import MPI; print(MPI.Get_library_version())"
```

---

## 2. Uruchamianie programu

```bash
mpiexec -n N python program.py
```

- `-n N` — liczba procesów (np. `-n 6` uruchomi 6 procesów)
- Każdy proces dostaje własną kopię programu, własny rank (0 do N-1)

### Uruchamianie na wielu maszynach
```bash
mpiexec -n N --hostfile hosts.txt python program.py
```

Plik `hosts.txt`:
```
maszyna1 slots=4
maszyna2 slots=4
```

---

## 3. Podstawy — rank, size, komunikator

```python
from mpi4py import MPI

comm = MPI.COMM_WORLD   # komunikator — grupa wszystkich procesów
rank = comm.Get_rank()   # numer tego procesu (0 do N-1) — odpowiednik PID w algorytmie
size = comm.Get_size()   # łączna liczba procesów N
```

- `MPI.Init()` i `MPI.Finalize()` są wywoływane **automatycznie** — nie trzeba ich ręcznie wołać
- `MPI.Init()` wykonuje się przy `import MPI`
- `MPI.Finalize()` wykonuje się przy wyjściu z programu

---

## 4. Wysyłanie i odbieranie wiadomości (punkt-punkt)

mpi4py ma **dwa zestawy metod**:

| Cecha | Małe litery (`send/recv`) | Wielkie litery (`Send/Recv`) |
|-------|---------------------------|-------------------------------|
| Dane | Dowolny obiekt Pythona | Bufory (NumPy arrays) |
| Serializacja | Automatyczna (wewnętrzna) | Brak — surowa pamięć |
| Szybkość | Wolniejsze | Szybsze |
| Użycie w projekcie | **TAK — to jest nasz wybór** | Niepotrzebne |

**W tym projekcie używamy wyłącznie metod z małej litery** — wysyłamy słowniki/krotki Pythona, co jest wygodne i wystarczająco szybkie.

---

### 4.1 Blokujące wysyłanie: `comm.send()`

```python
comm.send(obj, dest, tag=0)
```

| Parametr | Typ | Opis |
|----------|-----|------|
| `obj` | dowolny obiekt Pythona | dane do wysłania |
| `dest` | `int` | rank procesu docelowego |
| `tag` | `int` (domyślnie 0) | etykieta typu wiadomości |

Blokuje do momentu aż bufor wysyłki zostanie zwolniony (wiadomość została zbuforowana lub odebrana).

**Przykład:**
```python
data = {"type": "REQ_TARAS", "timestamp": 5, "pid": rank}
comm.send(data, dest=3, tag=1)
```

---

### 4.2 Blokujące odbieranie: `comm.recv()`

```python
obj = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=None)
```

| Parametr | Typ | Opis |
|----------|-----|------|
| `source` | `int` lub `MPI.ANY_SOURCE` | od kogo odbieramy (domyślnie: od kogokolwiek) |
| `tag` | `int` lub `MPI.ANY_TAG` | jaki tag akceptujemy (domyślnie: dowolny) |
| `status` | `MPI.Status` lub `None` | obiekt do zapisania metadanych o wiadomości |
| **zwraca** | obiekt Pythona | odebrane dane |

Blokuje do momentu odebrania pasującej wiadomości.

**Przykład — odbierz od kogokolwiek, dowolny tag:**
```python
status = MPI.Status()
data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

nadawca = status.Get_source()   # rank nadawcy
typ_msg = status.Get_tag()      # tag wiadomości
```

**Przykład — odbierz tylko od procesu 2, tylko tag 5:**
```python
data = comm.recv(source=2, tag=5)
```

---

### 4.3 Nieblokujące wysyłanie: `comm.isend()`

```python
request = comm.isend(obj, dest, tag=0)
```

Zwraca natychmiast obiekt `Request`. Wysyłka odbywa się w tle.

```python
req = comm.isend(data, dest=3, tag=1)
# ... można robić inne rzeczy ...
req.wait()   # czekaj na zakończenie wysyłki
```

---

### 4.4 Nieblokujące odbieranie: `comm.irecv()`

```python
request = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
```

Zwraca natychmiast obiekt `Request`. Odbiór odbywa się w tle.

```python
req = comm.irecv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)

# Sposób 1: czekaj blokująco
data = req.wait()

# Sposób 2: sprawdź czy już przyszło (nieblokująco)
gotowe, data = req.test()
if gotowe:
    # data zawiera odebrane dane
    pass
```

---

## 5. Obiekt `Request` — zarządzanie operacjami nieblokującymi

Zwracany przez `isend()` i `irecv()`.

### Metody instancji

| Metoda | Zwraca | Opis |
|--------|--------|------|
| `req.wait(status=None)` | dane (dla irecv) lub `None` (dla isend) | Blokuje do zakończenia operacji |
| `req.test(status=None)` | `(bool, dane lub None)` | Sprawdza bez blokowania czy operacja się zakończyła |
| `req.cancel()` | `None` | Anuluje operację |

### Metody klasowe (operacje na wielu requestach)

| Metoda | Zwraca | Opis |
|--------|--------|------|
| `Request.waitall(requests)` | `list[dane]` | Czeka na zakończenie **wszystkich** |
| `Request.waitany(requests)` | `(indeks, dane)` | Czeka na zakończenie **któregokolwiek** |
| `Request.testall(requests)` | `(bool, list lub None)` | Sprawdza czy **wszystkie** zakończone |
| `Request.testany(requests)` | `(indeks, bool, dane)` | Sprawdza czy **którykolwiek** zakończony |

**Przykład — wyślij do wszystkich i czekaj na zakończenie:**
```python
requests = []
for dest in range(size):
    if dest != rank:
        req = comm.isend(data, dest=dest, tag=TAG_REQ_TARAS)
        requests.append(req)
MPI.Request.waitall(requests)
```

---

## 6. Obiekt `Status` — metadane odebranej wiadomości

Tworzysz go sam, przekazujesz do `recv()` lub `probe()`, a MPI wypełnia go danymi.

```python
status = MPI.Status()
data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
```

### Metody / właściwości

| Metoda / właściwość | Zwraca | Opis |
|---------------------|--------|------|
| `status.Get_source()` lub `status.source` | `int` | Rank nadawcy |
| `status.Get_tag()` lub `status.tag` | `int` | Tag wiadomości |
| `status.Get_error()` lub `status.error` | `int` | Kod błędu |
| `status.Get_count()` | `int` | Liczba odebranych bajtów |

---

## 7. Probe — sprawdzanie czy jest wiadomość bez odbierania

### `comm.probe()` — blokujące

```python
status = MPI.Status()
comm.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
# teraz wiemy kto wysłał (status.source) i jaki tag (status.tag)
# wiadomość nadal czeka w kolejce — trzeba ją odebrać recv()
data = comm.recv(source=status.source, tag=status.tag)
```

### `comm.iprobe()` — nieblokujące

```python
status = MPI.Status()
jest_wiadomosc = comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
if jest_wiadomosc:
    data = comm.recv(source=status.source, tag=status.tag)
```

| Metoda | Blokuje? | Zwraca | Opis |
|--------|----------|--------|------|
| `probe(source, tag, status)` | TAK | `True` (zawsze) | Czeka aż wiadomość będzie dostępna |
| `iprobe(source, tag, status)` | NIE | `bool` | Sprawdza czy wiadomość czeka |

---

## 8. Stałe MPI

| Stała | Wartość | Użycie |
|-------|---------|--------|
| `MPI.ANY_SOURCE` | specjalna | Odbierz od dowolnego procesu |
| `MPI.ANY_TAG` | specjalna | Akceptuj dowolny tag |
| `MPI.COMM_WORLD` | komunikator | Domyślna grupa wszystkich procesów |

---

## 9. Zegar Lamporta — implementacja

mpi4py nie ma wbudowanego zegara Lamporta — trzeba go zaimplementować samemu. Ale to proste:

```python
clock = 0

def lamport_send(comm, obj, dest, tag):
    global clock
    clock += 1
    obj["timestamp"] = clock
    comm.send(obj, dest=dest, tag=tag)

def lamport_recv(comm, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
    global clock
    status = MPI.Status()
    obj = comm.recv(source=source, tag=tag, status=status)
    clock = max(clock, obj["timestamp"]) + 1
    return obj, status

def lamport_event():
    global clock
    clock += 1
```

---

## 10. Tagi wiadomości — mapowanie typów z algorytmu

Algorytm definiuje 9 typów wiadomości. Mapujemy je na tagi MPI:

```python
TAG_REQ_TARAS      = 1
TAG_ACK_TARAS      = 2
TAG_REL_TARAS      = 3
TAG_REQ_WINDA_DOL  = 4
TAG_ACK_WINDA_DOL  = 5
TAG_REL_WINDA_DOL  = 6
TAG_REQ_WINDA_GORA = 7
TAG_ACK_WINDA_GORA = 8
TAG_REL_WINDA_GORA = 9
```

---

## 11. Wzorce komunikacji potrzebne w projekcie

### 11.1 Broadcast ręczny (wyślij do wszystkich) — zamiast MPI_Bcast

```python
def send_to_all(comm, obj, tag):
    rank = comm.Get_rank()
    size = comm.Get_size()
    for dest in range(size):
        if dest != rank:
            comm.send(obj, dest=dest, tag=tag)
```

Używamy pętli z `send()` zamiast `comm.bcast()`, ponieważ **komunikacja kolektywna jest zabroniona** w wymaganiach projektu.

### 11.2 Pętla główna — nasłuchiwanie + reagowanie

Rdzeń algorytmu to pętla, w której proces czeka na wiadomości i reaguje na nie w zależności od swojego stanu:

```python
while True:
    status = MPI.Status()
    msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
    sender = status.Get_source()
    tag = status.Get_tag()

    # aktualizacja zegara Lamporta
    clock = max(clock, msg["timestamp"]) + 1

    if tag == TAG_REQ_TARAS:
        handle_req_taras(sender, msg)
    elif tag == TAG_ACK_TARAS:
        handle_ack_taras(sender, msg)
    elif tag == TAG_REL_TARAS:
        handle_rel_taras(sender, msg)
    # ... itd. dla pozostałych tagów
```

### 11.3 Wysyłanie wiadomości z danymi strukturalnymi

Wiadomości mogą być słownikami z dowolną zawartością:

```python
# REQ — zawiera timestamp i PID
msg = {"timestamp": clock, "pid": rank}
comm.send(msg, dest=dest, tag=TAG_REQ_TARAS)

# REL_WINDA_DOL — zawiera timestamp i listę PID[] grupy
msg = {"timestamp": clock, "pid_list": [0, 3, 5]}
comm.send(msg, dest=dest, tag=TAG_REL_WINDA_DOL)

# ACK — wystarczy sam timestamp
msg = {"timestamp": clock}
comm.send(msg, dest=dest, tag=TAG_ACK_TARAS)
```

### 11.4 Nieblokujące sprawdzanie wiadomości w pętli z logiką lokalną

Jeśli proces musi robić coś między wiadomościami (np. sprawdzać warunki wejścia do windy):

```python
while True:
    # sprawdź czy jest wiadomość
    if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        handle_message(msg, status)

    # sprawdź warunki lokalne
    check_conditions()
```

---

## 12. Format kolejki priorytetowej

Algorytm wymaga kolejek posortowanych po `(timestamp, PID)`. W Pythonie:

```python
import bisect

queue_taras = []  # lista krotek (timestamp, pid)

# dodanie żądania (posortowane rosnąco)
bisect.insort(queue_taras, (timestamp, pid))

# usunięcie żądania
queue_taras.remove((timestamp, pid))

# pozycja procesu w kolejce (1-indexed)
pozycja = queue_taras.index((my_timestamp, rank)) + 1

# pierwsze W procesów
pierwsza_grupa = queue_taras[:W]

# sprawdzenie czy proces jest w pierwszej grupie
czy_w_grupie = (my_timestamp, rank) in queue_taras[:W]
```

---

## 13. Logowanie — wymagany format

```python
def log(rank, clock, message):
    print(f"[{rank}] [t{clock}] {message}", flush=True)
```

Parametr `flush=True` jest ważny — bez niego logi z różnych procesów mogą się buforować i wyświetlać w złej kolejności.

---

## 14. Kompletny szkielet programu

```python
from mpi4py import MPI
import bisect
import time
import random

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# parametry
W = 3  # pojemność windy
T = 6  # pojemność tarasu

# tagi wiadomości
TAG_REQ_TARAS      = 1
TAG_ACK_TARAS      = 2
TAG_REL_TARAS      = 3
TAG_REQ_WINDA_DOL  = 4
TAG_ACK_WINDA_DOL  = 5
TAG_REL_WINDA_DOL  = 6
TAG_REQ_WINDA_GORA = 7
TAG_ACK_WINDA_GORA = 8
TAG_REL_WINDA_GORA = 9

# stany
NA_DOLE             = "NA_DOLE"
CZEKA_NA_WINDE_DOL  = "CZEKA_NA_WINDE_DOL"
NA_TARASIE          = "NA_TARASIE"
CZEKA_NA_WINDE_GORA = "CZEKA_NA_WINDE_GORA"

# zmienne procesu
clock = 0
state = NA_DOLE
queue_taras = []       # lista (timestamp, pid)
queue_winda_dol = []   # lista (timestamp, pid)
queue_winda_gora = []  # lista (timestamp, pid)
queue_request = []     # lista (pid, tag_ack) — procesy czekające na opóźniony ACK
ack_taras = 0
ack_winda_dol = 0
ack_winda_gora = 0
my_req_ts = None       # timestamp mojego bieżącego żądania


def log(msg):
    print(f"[{rank}] [t{clock}] {msg}", flush=True)


# --- Pętla główna procesu ---
# Każdy proces w nieskończonej pętli:
#   1. NA_DOLE: losowy odpoczynek, potem REQ_TARAS do wszystkich
#   2. Nasłuchuje wiadomości i reaguje wg algorytmu
#   3. Sprawdza warunki przejścia między stanami
#   4. Po zjechaniu windą: REL_TARAS, powrót do NA_DOLE
```

---

## 15. Podsumowanie — co oferuje mpi4py vs wymagania projektu

| Wymaganie projektu | Funkcja mpi4py | Status |
|--------------------|----------------|--------|
| MPI_Send / MPI_Recv (lub async) | `comm.send()` / `comm.recv()` / `comm.isend()` / `comm.irecv()` | OK |
| Tagi wiadomości (9 typów) | parametr `tag` we wszystkich metodach | OK |
| Odbieranie od dowolnego procesu | `MPI.ANY_SOURCE`, `MPI.ANY_TAG` | OK |
| Sprawdzanie nadawcy i tagu po odbiorze | `MPI.Status` -> `Get_source()`, `Get_tag()` | OK |
| Wysyłanie struktur (timestamp, PID, PID[]) | dowolne obiekty Pythona przez serializację | OK |
| Rank procesu = PID | `comm.Get_rank()` | OK |
| Liczba procesów N | `comm.Get_size()` | OK |
| Uruchamianie na wielu maszynach | `mpiexec --hostfile` | OK |
| Brak komunikacji kolektywnej | Nie używamy `bcast()` / `allreduce()` — robimy pętle z `send()` | OK |
| Zegar Lamporta | Implementujemy ręcznie (patrz sekcja 9) | OK |
| Probe (sprawdzanie wiadomości bez blokowania) | `comm.iprobe()` / `comm.probe()` | OK |
