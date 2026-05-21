# Taras widokowy — algorytm rozproszony

Algorytm rozproszony rozwiazujacy problem tarasu widokowego z wykorzystaniem MPI (point-to-point).

## Problem

Mieszkancy wioski chca wjezdzac winda na taras widokowy. Ograniczenia:
- Winda ma pojemnosc **W** i rusza tylko gdy jest pelna
- Taras ma pojemnosc **T** (T > W)
- Winda jest jedna i wspoldzielona
- Procesy dzialaja w petli: wjazd na gore, pobyt, zjazd na dol

## Algorytm

Oparty na uogolnionym algorytmie Lamporta (sekcja krytyczna o pojemnosci > 1). Trzy sekcje krytyczne:
1. **TARAS** (pojemnosc T) — rezerwacja miejsca
2. **WINDA_DOL** (pojemnosc W) — wjazd na gore
3. **WINDA_GORA** (pojemnosc W) — zjazd na dol

Kazda sekcja korzysta z mechanizmu REQ/ACK/REL i zegarow Lamporta do ustalania kolejnosci.

## Uruchomienie

```bash
mpiexec -n <liczba_procesow> python main.py
```

Wymagania:
- Python 3
- mpi4py (`pip install mpi4py`)
- Implementacja MPI (np. OpenMPI, MPICH)

## Parametry

Zdefiniowane na poczatku `main.py`:
- `W` — pojemnosc windy
- `T` — pojemnosc tarasu
