from mpi4py import MPI  
import time, random

# Stałe
comm = MPI.COMM_WORLD                                                                                                                                                                            
pid = comm.Get_rank()  # ID procesu np. 3
N = comm.Get_size()    # liczba procesów

W = N - 1 # Pojemnosc windy
T = 2 # Pojemnosc tarasu

REQ_TARAS = 1
ACK_TARAS = 2
REQ_WINDA_DOL = 3
ACK_WINDA_DOL = 4
REL_WINDA_DOL = 5
REQ_WINDA_GORA = 6
ACK_WINDA_GORA = 7
REL_WINDA_GORA = 8
REL_TARAS = 9

# Funkcje pomocnicze
def wait():
    time.sleep(random.uniform(1, 5))  # czeka 1-5 sekund

# Wysyłam wiadomość do wszystkich
def send_to_all(msg, tag):
  for dest in range(N):
      if dest != pid:                                                                                                                                                                               
          comm.send(msg, dest=dest, tag=tag)



clock = 0
stan = "NA_DOLE" # (NA_DOLE, CZEKA_NA_WINDE_DOL, CZEKA_NA_WINDE_GORA, NA_TARASIE)

# Kolejki:
kolejka_taras = []
kolejka_winda_dol = []
kolejka_winda_gora = []
kolejka_request = []

# Liczniki
ack_taras = 0
ack_winda_gora = 0
ack_winda_dol = 0

# Wysyłam REQ_TARAS do wszystkich
wait()
ack_taras = 0
clock += 1 # Zwiekszam zegar lamporta
send_to_all({"pid": pid, "timestamp": clock}, REQ_TARAS)
stan = "CZEKA_NA_TARAS"

clock_req_taras = clock
kolejka_taras.append({"pid": pid, "timestamp": clock_req_taras})
kolejka_taras.sort(key=lambda x: (x['timestamp'], x['pid']))

print(f"{pid}: wysyłam wszystkim REQ_TARAS")

while True: 
    # Dostaje wiadomość 
    status = MPI.Status()
    msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
    tag = status.Get_tag()
    nadawca = status.Get_source()

    # Aktualizuje swoj zegar lamporta
    clock = max(clock, msg['timestamp']) + 1

    # Sprawdzam co to za wiadomość i reaguje
    if tag == 1:
        # Dostaje żądanie REQ_TARAS
        print(f"{pid}: dostaje REQ_TARAS od {nadawca}")

        # Dodaje zadanie nadawcy do kolejka_taras (timestamp i pid)
        kolejka_taras.append(msg)
        kolejka_taras.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f"KOLEJKA NA TARAS: {kolejka_taras}")

        # odsyłam dokłądnie temu wątkowi ACK_TARAS
        clock += 1
        comm.send({"pid": pid, "timestamp": clock}, dest=nadawca, tag=ACK_TARAS)

    elif tag == 2:
        # Dostaje ACK_TARAS
        print(f"{pid}: dostaje ACK_TARAS od {nadawca}")

        ack_taras += 1 # Zwiekszam o 1

        pozycja_w_kolejka_taras = kolejka_taras.index({"pid": pid, "timestamp": clock_req_taras}) + 1
        print(f"{pid}: moja pozycja w kolejce na taras to {pozycja_w_kolejka_taras}")

        # Sprawdzam czy mam juz n-1 acków
        if ack_taras == N-1 and pozycja_w_kolejka_taras <= min(T-(T%W), N-(N%W)):
            print(f"{pid}: mam juz N-1 ack! ")

            ack_winda_dol = 0

            # Wysyłam REQ_WINDA_DOL
            clock += 1
            clock_req_winda_dol = clock
            send_to_all({"pid": pid, "timestamp": clock}, REQ_WINDA_DOL)

            # Ustawiam swoj stan na CZEKA_NA_WINDE_DOL
            stan = "CZEKA_NA_WINDE_DOL"

            # Dodaje siebie do kolejka_winda_dol???
            kolejka_winda_dol.append({"pid": pid, "timestamp": clock})
            kolejka_winda_dol.sort(key=lambda x: (x['timestamp'], x['pid']))

    elif tag == 3:
        # Dostaje REQ_WINDA_DOL
        print(f"{pid}: dostaje REQ_WINDA_DOL od {nadawca}")

        # Dodaje zadanie nadawcy do kolejka_taras (timestamp i pid)
        kolejka_winda_dol.append(msg)
        kolejka_winda_dol.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f"KOLEJKA WINDA DOL: {kolejka_winda_dol}")

        # odsyłam dokłądnie temu wątkowi ACK_TARAS
        clock += 1
        comm.send({"pid": pid, "timestamp": clock}, dest=nadawca, tag=ACK_WINDA_DOL)

    elif tag == 4:
        # Dostaje ACK_WINDA_DOL
        print(f"{pid}: dostaje ACK_WINDA_DOL od {nadawca}")

        ack_winda_dol += 1 # Zwiekszam o 1

        pozycja_w_kolejka_winda_dol = kolejka_winda_dol.index({"pid": pid, "timestamp": clock_req_winda_dol}) + 1
        print(f"{pid}: moja pozycja w kolejce na taras to {pozycja_w_kolejka_winda_dol}")

        # Sprawdzam czy pierwszy proces w kolejce queue_winda_dol posiada większy priorytet niz pierwszy proces w kolejce queue_winda_gora.
        priorytet_winda_dol = kolejka_winda_dol[0]['timestamp']

        if len(kolejka_winda_gora) > 0:
            priorytet_winda_gora = kolejka_winda_gora[0]['timestamp']
        else:
            priorytet_winda_gora = float('inf')

        # Sprawdzam czy mam juz n-1 acków
        if ack_winda_dol == N-1 and pozycja_w_kolejka_winda_dol <= W and priorytet_winda_dol < priorytet_winda_gora:
            print(f"{pid}: mam juz N-1 ack! Wysylam REL_WINDA_DOL!")

            # Pobieram liste pierwszych W procesów z kolejka_winda_dol
            pidy_do_windy = list()
            i = 0
            while i < W:
                pidy_do_windy.append(kolejka_winda_dol[i]['pid'])
                i += 1

            # Wysyłam REL_WINDA_DOL
            clock += 1
            clock_rel_winda_dol = clock
            send_to_all({"pid": pidy_do_windy, "timestamp": clock}, REL_WINDA_DOL)

    elif tag == 5:
        # Dostałem REL_WINDA_DOL
        pidy_w_windzie = msg['pid']
        kolejka_winda_dol = [x for x in kolejka_winda_dol if x['pid'] not in pidy_w_windzie]

        if pid in pidy_w_windzie:
            # Mój pid znajduje się w windzie
            stan = "NA_TARASIE"

            wait()

            ack_winda_gora = 0

            # Wysyłam REQ_WINDA_GORA
            clock += 1
            clock_req_winda_gora = clock
            send_to_all({"pid": pid, "timestamp": clock}, REQ_WINDA_GORA)

            # Ustawiam swoj stan na CZEKA_NA_WINDE_GORA
            stan = "CZEKA_NA_WINDE_GORA"

            # Dodaje siebie do kolejka_winda_gora???
            kolejka_winda_gora.append({"pid": pid, "timestamp": clock})
            kolejka_winda_gora.sort(key=lambda x: (x['timestamp'], x['pid']))

    elif tag == 6:
        # Dostaje REQ_WINDA_GORA
        print(f"{pid}: dostaje REQ_WINDA_GORA od {nadawca}")

        kolejka_winda_gora.append(msg)
        kolejka_winda_gora.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f"KOLEJKA WINDA GORA: {kolejka_winda_gora}")

        clock += 1
        comm.send({"pid": pid, "timestamp": clock}, dest=nadawca, tag=ACK_WINDA_GORA)

    elif tag == 7:
        # Dostaje ACK_WINDA_GORA
        print(f"{pid}: dostaje ACK_WINDA_GORA od {nadawca}")

        ack_winda_gora += 1 # Zwiekszam o 1

        pozycja_w_kolejka_winda_gora = kolejka_winda_gora.index({"pid": pid, "timestamp": clock_req_winda_gora}) + 1
        print(f"{pid}: moja pozycja w kolejce na taras to {pozycja_w_kolejka_winda_gora}")

        # Sprawdzam czy pierwszy proces w kolejce queue_winda_gora posiada większy priorytet niz pierwszy proces w kolejce queue_winda_gora.
        priorytet_winda_gora = kolejka_winda_gora[0]['timestamp']

        if len(kolejka_winda_dol) > 0:
            priorytet_winda_dol = kolejka_winda_dol[0]['timestamp']
        else:
            priorytet_winda_dol = float('inf')

        # Sprawdzam czy mam juz n-1 acków
        if ack_winda_gora == N-1 and pozycja_w_kolejka_winda_gora <= W and priorytet_winda_gora < priorytet_winda_dol:
            print(f"{pid}: mam juz N-1 ack! Wysylam REL_WINDA_GORA!")

            # Pobieram liste pierwszych W procesów z kolejka_winda_dol
            pidy_do_windy = list()
            i = 0
            while i < W:
                pidy_do_windy.append(kolejka_winda_gora[i]['pid'])
                i += 1

            # Wysyłam REL_WINDA_GORA
            clock += 1
            clock_rel_winda_gora = clock
            send_to_all({"pid": pidy_do_windy, "timestamp": clock}, REL_WINDA_GORA)

    elif tag == 8:
        # Dostałem REL_WINDA_GORA
        pidy_w_windzie = msg['pid']
        kolejka_winda_gora = [x for x in kolejka_winda_gora if x['pid'] not in pidy_w_windzie]

        if pid in pidy_w_windzie:
            # Mój pid znajduje się w windzie
            stan = "NA_DOLE"

            wait()

            # Wysyłam REL_TARAS
            clock += 1
            clock_rel_taras = clock
            send_to_all({"pid": pid, "timestamp": clock}, REL_TARAS)

            # Powrót do początku cyklu (punkt 2)
            wait()
            ack_taras = 0
            clock += 1
            clock_req_taras = clock
            send_to_all({"pid": pid, "timestamp": clock}, REQ_TARAS)
            stan = "CZEKA_NA_TARAS"
            kolejka_taras.append({"pid": pid, "timestamp": clock_req_taras})
            kolejka_taras.sort(key=lambda x: (x['timestamp'], x['pid']))
            print(f"{pid}: wysyłam wszystkim REQ_TARAS")

    elif tag == 9:
        # Dostaje REL_TARAS
        print(f"{pid}: dostaje REL_TARAS od {nadawca}")

        kolejka_taras = [x for x in kolejka_taras if x['pid'] != msg['pid']]
        print(f"KOLEJKA NA TARAS: {kolejka_taras}")
