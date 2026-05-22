# Analiza main.py vs algorytm_taras_widokowy.md

## 1. [KRYTYCZNY] Brak ponownego sprawdzenia warunków po REL_WINDA_DOL / REL_WINDA_GORA

**Problem:** Algorytm (7.2.2.2, 7.2.3.2) mówi, że po otrzymaniu REL_WINDA_DOL/GORA należy ponownie sprawdzić warunki wejścia do windy (6.2.5 / 6.2.8). W kodzie warunki te sprawdzane są TYLKO w handlerach ACK_WINDA_DOL (linia 236) i ACK_WINDA_GORA (linia 312). Gdy grupa odjedzie i kolejka się przesunie, nowy W-ty proces nigdy nie sprawdzi swoich warunków — możliwe zakleszczenie.

**Propozycja naprawy:** Po usunięciu wpisów z kolejki w handlerach REL_WINDA_DOL i REL_WINDA_GORA, jeśli proces jest w odpowiednim stanie (CZEKA_NA_WINDE_DOL / CZEKA_NA_WINDE_GORA) i ma ack == N-1, ponownie sprawdź pozycję w kolejce i warunki priorytetu windy. Wydziel logikę sprawdzania warunków do osobnej metody, żeby nie duplikować kodu.

---

## 2. [KRYTYCZNY] REL_TARAS zakomentowany w REL_WINDA_GORA

**Problem:** Algorytm (punkt 9): każdy proces po REL_WINDA_GORA z własnym PID zmienia stan na NA_DOLE **i wysyła REL_TARAS**. W kodzie (linie 360-363) jest to zakomentowane. Tylko W-ty proces wysyła grupowy REL_TARAS (linia 341). Funkcjonalnie to może działać, ale: (a) odbiega od specyfikacji, (b) jeśli REL_TARAS dotrze przed REL_WINDA_GORA do jakiegoś procesu, może powstać niespójność.

**Propozycja naprawy:** Są dwa podejścia:
- (A) Zostawić grupowy REL_TARAS wysyłany przez W-ty proces — prostsze, mniej wiadomości, ale trzeba udowodnić poprawność.
- (B) Odkomentować REL_TARAS w handlerze REL_WINDA_GORA, żeby każdy proces sam wysyłał swoje zwolnienie — zgodne ze specyfikacją. Trzeba wtedy usunąć grupowy REL_TARAS z handlera ACK_WINDA_GORA (linie 340-343).

---

## 3. [KRYTYCZNY] Brak tie-breakingu po PID w porównaniach priorytetów

**Problem:** Algorytm: "mniejszy timestamp = wyższy priorytet. Przy równych timestampach: mniejszy PID = wyższy priorytet." Kod (linia 157, 209, 286) porównuje tylko timestamp, nie uwzględniając PID. Dwa procesy z tym samym timestamp mogą się wzajemnie blokować.

**Propozycja naprawy:** Zamienić porównanie `msg['timestamp'] > queue[index]['timestamp']` na porównanie krotek: `(msg['timestamp'], msg['pid']) > (queue[index]['timestamp'], queue[index]['pid'])`. Python porównuje krotki leksykograficznie, więc to automatycznie obsłuży tie-breaking.

---

## 4. [MNIEJSZY] Odroczone żądania nie trafiają do kolejki zasobowej

**Problem:** Gdy proces odracza ACK (dodaje do queue_request), nie dodaje żądania do queue_taras/queue_winda_dol/queue_winda_gora. Odroczone procesy mają niższy priorytet, więc nie wpływają na pozycję odraczającego procesu. Ale lokalna kolejka zasobowa jest niekompletna.

**Propozycja naprawy:** Dodawać żądanie do OBIE kolejek — queue_request (do późniejszego odesłania ACK) i odpowiedniej queue_X (do utrzymania spójnego widoku kolejki). Przy flush queue_request NIE trzeba ponownie dodawać do queue_X, bo już tam jest.

---

## 5. [MNIEJSZY] Literówka w princie

**Problem:** Linia 88: `message="KOLEJKA WINDA DOL:"` ale dodaje do `kolejka_winda_gora`.

**Propozycja naprawy:** Zmienić na `"KOLEJKA WINDA GORA:"`.

---

## 6. [MNIEJSZY] Zmienne klasowe zamiast instancyjnych

**Problem:** Linie 26-43: listy zdefiniowane na poziomie klasy (współdzielone między instancjami w standardowym Pythonie). Z MPI nie powoduje problemów, ale jest niepoprawne.

**Propozycja naprawy:** Przenieść inicjalizację wszystkich zmiennych mutowalnych (listy, liczniki) do metody `__init__`.

---

## 7. [KRYTYCZNY] Deadlock przy równych priorytetach kierunków windy

**Problem:** W liniach 244 i 322 porównanie priorytetów kierunków windy używa operatora ścisłego (`<`) wyłącznie na timestampach. Linia 244: `if ts_winda_dol < ts_winda_gora:`, linia 322: `if ts_winda_gora < ts_winda_dol:`. Jeśli pierwszy proces w `queue_winda_dol` i pierwszy proces w `queue_winda_gora` mają ten sam timestamp, żaden warunek nie jest spełniony — winda nie ruszy w żadnym kierunku. To prowadzi do deadlocka.

**Propozycja naprawy:** Porównywać krotki `(timestamp, PID)` zamiast samych timestampów. Ponieważ PID jest unikalny, krotki są zawsze różne i dokładnie jeden kierunek wygrywa. Np.: `if (ts_dol, pid_dol) < (ts_gora, pid_gora):` dla kierunku dół.

---

## 8. [KRYTYCZNY] Brak ponownego sprawdzenia warunków po REL_TARAS

**Problem:** Analogicznie do punktu 1, ale dotyczy kolejki tarasu. Proces w stanie `NA_DOLE` z `ack_taras == N-1`, którego pozycja w `queue_taras` przekracza limit `min(T-(T%W), N-(N%W))`, nie przejdzie do `CZEKA_NA_WINDE_DOL`. Po otrzymaniu `REL_TARAS` (linie 368-373) pozycja procesu w kolejce może się poprawić, ale warunki przejścia nigdy nie są ponownie sprawdzane — są sprawdzane wyłącznie w handlerze `ACK_TARAS` (linia 186). Możliwe zakleszczenie.

**Propozycja naprawy:** Po usunięciu wpisów z `queue_taras` w handlerze `REL_TARAS`, jeśli proces jest w stanie `NA_DOLE` i ma `ack_taras == N-1`, ponownie sprawdzić pozycję w kolejce i warunki przejścia do `CZEKA_NA_WINDE_DOL`.
