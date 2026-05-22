from mpi4py import MPI  
import time, random

REQ_TARAS = 1
ACK_TARAS = 2
REQ_WINDA_DOL = 3
ACK_WINDA_DOL = 4
REL_WINDA_DOL = 5
REQ_WINDA_GORA = 6
ACK_WINDA_GORA = 7
REL_WINDA_GORA = 8
REL_TARAS = 9

# Stałe
comm = MPI.COMM_WORLD
N = comm.Get_size()    # liczba procesów
W = 3 # Pojemnosc windy
T = N - 1 # Pojemnosc tarasu

# Funkcje pomocnicze
def wait():
    time.sleep(random.uniform(1, 5))  # czeka 1-5 sekund

class Process:
    # Zmienne procesu
    pid = -1
    stan = "NA_DOLE" # (NA_DOLE, CZEKA_NA_WINDE_DOL, CZEKA_NA_WINDE_GORA, NA_TARASIE)
    clock = 0

    # Kolejki:
    kolejka_taras = []
    kolejka_winda_dol = []
    kolejka_winda_gora = []
    kolejka_request = []

    # Liczniki
    ack_taras = 0
    ack_winda_gora = 0
    ack_winda_dol = 0

    # Czas pobudki
    wakeup_time = 0
    is_sleeping = False

    def __init__(self, pid):
        self.pid = pid

    # Wyslanie wiadomosci do pozostalych procesow
    def send_to_all(self, msg, tag):
        for dest in range(N):
            if dest != self.pid:
                comm.send(msg, dest=dest, tag=tag)

    # Dodanie samego siebie do kolejki
    def add_self_to_queue(self, queue, message="KOLEJKA:"):
        queue.append({'pid': self.pid, 'timestamp': self.clock})
        queue.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f'{self.pid} >> {message} {queue}')

    # Dodanie calej wiadomosci do kolejki
    def add_to_queue(self, queue, msg, message="KOLEJKA:"):
        queue.append(msg)
        queue.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f'{self.pid} >> {message} {queue}')

    def get_index_by_pid(self, queue, pid):
        for i in range(len(queue)):
            if queue[i]['pid'] == pid:
                return i
        return -1

    def sleep(self):
        self.wakeup_time = time.time() + random.uniform(2, 5)
        self.is_sleeping = True

    def NA_DOLE_wakeup(self):
        self.ack_taras = 0
        self.clock += 1
        self.send_to_all({"pid": self.pid, "timestamp": self.clock}, REQ_TARAS)
        self.add_self_to_queue(queue=self.kolejka_taras, message="KOLEJKA TARAS:")
        print(f"{self.pid} >> Wysyłam wszystkim REQ_TARAS")

    def NA_TARASIE_wakeup(self):
        self.ack_winda_gora = 0
        self.clock += 1
        self.stan = "CZEKA_NA_WINDE_GORA"
        self.send_to_all({"pid": self.pid, "timestamp": self.clock}, REQ_WINDA_GORA)
        self.add_self_to_queue(queue=self.kolejka_winda_gora, message="KOLEJKA WINDA DOL:")



if __name__ == '__main__':
    p = Process(pid=comm.Get_rank())  # Obiekt procesu
    if p.pid == 0:
        p.NA_DOLE_wakeup()
    else:
        p.sleep()
        print(f'{p.pid} >> Czeka')


    # Odbieranie wiadomosci w petli
    while True:
        # Sprawdzenie czy jest wiadomosc do odebrania
        status = MPI.Status()
        ready = comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

        if not ready:
            # Sprawdzenie czy proces sie 'wybudzil'
            if time.time() >= p.wakeup_time and p.is_sleeping:
                if p.stan == "NA_DOLE":
                    p.NA_DOLE_wakeup()
                    p.is_sleeping = False
                elif p.stan == "NA_TARASIE":
                    p.NA_TARASIE_wakeup()
                    p.is_sleeping = False

            time.sleep(0.1)
            continue

        sender, tag = status.Get_source(), status.Get_tag()
        msg = comm.recv(source=sender, tag=tag)

        '''
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        sender = status.Get_source()
        '''

        # Lokalna synchronizacja zegara
        p.clock = max(p.clock, msg['timestamp']) + 1


        # Sprawdzenie otrzymanej wiadomosci
        if tag == REQ_TARAS:
            print(f"{p.pid} >> Dostalem REQ_TARAS od {sender}")
            index = p.get_index_by_pid(p.kolejka_taras, p.pid)
            # if (p.stan != "NA_DOLE" or
            #     p.ack_taras == N - 1 or
            #     #(p.stan == "NA_DOLE" and index == -1) or
            #     (index != -1 and msg['timestamp'] <= p.kolejka_taras[index]['timestamp'])):
            #     # Dodanie wiadomosci do kolejka_taras
            #     p.add_to_queue(p.kolejka_taras, msg, "KOLEJKA NA TARAS:")
            #
            #     p.clock += 1
            #     comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_TARAS)
            # else:
            #     # Dodanie wiadomosci do kolejka_request
            #     p.add_to_queue(p.kolejka_request, msg, "KOLEJKA REQUEST:")
            if p.stan != "NA_DOLE" or p.ack_taras == N - 1:
                # Dodanie wiadomosci do kolejka_taras
                p.add_to_queue(p.kolejka_taras, msg, "KOLEJKA NA TARAS:")

                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_TARAS)
            else:
                if index != -1 and msg['timestamp'] > p.kolejka_taras[index]['timestamp']:
                    # Dodanie wiadomosci do kolejka_request
                    p.add_to_queue(p.kolejka_request, msg, "KOLEJKA REQUEST:")
                else:
                    # Dodanie wiadomosci do kolejka_taras
                    p.add_to_queue(p.kolejka_taras, msg, "KOLEJKA NA TARAS:")

                    p.clock += 1
                    comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_TARAS)


        elif tag == ACK_TARAS:
            print(f"{p.pid} >> Dostalem ACK_TARAS od {sender}")

            if p.stan == "NA_DOLE":
                p.ack_taras += 1

                if p.ack_taras == N - 1:
                    # Odeslanie zaleglych ACK wszystkim z kolejka_request
                    while p.kolejka_request:
                        msg = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=msg['pid'], tag=ACK_TARAS)

                # Pozycja w kolejce taras
                pos_in_kolejka_taras = p.get_index_by_pid(p.kolejka_taras, p.pid) + 1
                print(f"{p.pid} >> Pozycja w kolejce na taras to {pos_in_kolejka_taras}")

                # Sprawdzenie warunkow
                if p.ack_taras == N - 1 and pos_in_kolejka_taras <= min(T - (T % W), N - (N % W)):
                    print(f"{p.pid} >> Zmieniam stan na CZEKA_NA_WINDE_DOL! ")

                    p.ack_winda_dol = 0
                    p.ack_taras = 0

                    # Zmiana stanu na CZEKA_NA_WINDE_DOL
                    p.stan = "CZEKA_NA_WINDE_DOL"

                    # Wysyłanie REQ_WINDA_DOL
                    p.clock += 1
                    p.send_to_all({"pid": p.pid, "timestamp": p.clock}, REQ_WINDA_DOL)

                    # Dodaje siebie do kolejka_winda_dol???
                    p.add_self_to_queue(p.kolejka_winda_dol, "KOLEJKA WINDA DOL:")


        elif tag == REQ_WINDA_DOL:
            print(f"{p.pid} >> Dostalem REQ_WINDA_DOL od {sender}")

            index = p.get_index_by_pid(p.kolejka_winda_dol, p.pid)
            if (p.stan != "CZEKA_NA_WINDE_DOL" or
                    p.ack_winda_dol == N - 1 or
                    (index != -1 and msg['timestamp'] <= p.kolejka_winda_dol[index]['timestamp'])):
                # Dodanie wiadomosci do kolejka_winda_dol
                p.add_to_queue(p.kolejka_winda_dol, msg, "KOLEJKA WINDA DOL:")

                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_WINDA_DOL)
            else:
                # Dodanie wiadomosci do kolejka_request
                p.add_to_queue(p.kolejka_request, msg, "KOLEJKA REQUEST:")

        elif tag == ACK_WINDA_DOL:
            print(f"{p.pid} >> Dostalem ACK_WINDA_DOL od {sender}")

            if p.stan == "CZEKA_NA_WINDE_DOL":
                p.ack_winda_dol += 1  # Zwiekszam o 1

                if p.ack_winda_dol == N - 1:
                    while p.kolejka_request:
                        msg = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=msg['pid'], tag=ACK_WINDA_DOL)


                pos_in_kolejka_winda_dol = p.get_index_by_pid(p.kolejka_winda_dol, p.pid) + 1
                print(f"{p.pid} >> Pozycja w kolejka_winda_dol {pos_in_kolejka_winda_dol}")

                # Sprawdzenie warunku przejscia CZEKA_NA_WINDE_DOL -> NA_TARASIE
                if p.ack_winda_dol == N - 1 and pos_in_kolejka_winda_dol == W:
                    # Sprawdzam czy pierwszy proces w kolejce queue_winda_dol posiada większy priorytet niz pierwszy proces w kolejce queue_winda_gora.
                    ts_winda_dol = p.kolejka_winda_dol[0]['timestamp']
                    ts_winda_gora = float('inf')

                    if len(p.kolejka_winda_gora) > 0:
                        ts_winda_gora = p.kolejka_winda_gora[0]['timestamp']

                    if ts_winda_dol < ts_winda_gora:
                        # Pobieram liste pierwszych W procesów z kolejka_winda_dol
                        pidy_do_windy = list()
                        i = 0
                        while i < W:
                            pidy_do_windy.append(p.kolejka_winda_dol[i]['pid'])
                            i += 1

                        print(f"{p.pid} >> Jestem W-tym procesem w kolejka_winda_dol. Wysylam grupowe REL_WINDA_DOL! {pidy_do_windy}")

                        # Wysyłam REL_WINDA_DOL
                        p.clock += 1
                        p.send_to_all({"pid": pidy_do_windy, "timestamp": p.clock}, REL_WINDA_DOL)

                        # Obsluga W-tego procesu
                        p.kolejka_winda_dol = [x for x in p.kolejka_winda_dol if x['pid'] not in pidy_do_windy]
                        p.stan = "NA_TARASIE"
                        p.sleep()
                        print(f'{p.pid} >> Czeka')


        elif tag == REL_WINDA_DOL:
            print(f"{p.pid} >> Dostalem REL_WINDA_DOL od {sender}")

            pidy_w_windzie = msg['pid']
            p.kolejka_winda_dol = [x for x in p.kolejka_winda_dol if x['pid'] not in pidy_w_windzie]

            if p.pid in pidy_w_windzie:
                # Pid procesu znajduje się w windzie
                p.stan = "NA_TARASIE"
                p.ack_winda_dol = 0

                p.sleep()
                print(f'{p.pid} >> Czeka')


        elif tag == REQ_WINDA_GORA:
            print(f"{p.pid} >> Dostalem REQ_WINDA_GORA od {sender}")

            index = p.get_index_by_pid(p.kolejka_winda_gora, p.pid)
            if (p.stan != "CZEKA_NA_WINDE_GORA" or
                    p.ack_winda_gora == N - 1 or
                    (index != -1 and msg['timestamp'] <= p.kolejka_winda_gora[index]['timestamp'])):
                # Dodanie wiadomosci do kolejka_winda_gora
                p.add_to_queue(p.kolejka_winda_gora, msg, "KOLEJKA WINDA GORA:")

                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_WINDA_GORA)
            else:
                # Dodanie wiadomosci do kolejka_request
                p.add_to_queue(p.kolejka_request, msg, "KOLEJKA REQUEST:")

        elif tag == ACK_WINDA_GORA:
            print(f"{p.pid} >> Dostalem ACK_WINDA_GORA od {sender}")

            if p.stan == "CZEKA_NA_WINDE_GORA":
                p.ack_winda_gora += 1  # Zwiekszam o 1

                if p.ack_winda_gora == N - 1:
                    while p.kolejka_request:
                        msg = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=msg['pid'], tag=ACK_WINDA_GORA)

                pos_in_kolejka_winda_gora = p.get_index_by_pid(p.kolejka_winda_gora, p.pid) + 1
                print(f"{p.pid} >> Pozycja w kolejka_winda_gora {pos_in_kolejka_winda_gora}")

                # Sprawdzenie warunku przejscia CZEKA_NA_WINDE_DOL -> NA_TARASIE
                if p.ack_winda_gora == N - 1 and pos_in_kolejka_winda_gora == W:
                    # Sprawdzam czy pierwszy proces w kolejce queue_winda_dol posiada większy priorytet niz pierwszy proces w kolejce queue_winda_gora.
                    ts_winda_gora = p.kolejka_winda_gora[0]['timestamp']
                    ts_winda_dol = float('inf')

                    if len(p.kolejka_winda_dol) > 0:
                        ts_winda_dol = p.kolejka_winda_dol[0]['timestamp']

                    print(f'{p.pid} >> KOLEJKA WINDA DOL: {p.kolejka_winda_dol}')

                    if ts_winda_gora < ts_winda_dol:
                        # Pobieram liste pierwszych W procesów z kolejka_winda_gora
                        pidy_do_windy = list()
                        i = 0
                        while i < W:
                            pidy_do_windy.append(p.kolejka_winda_gora[i]['pid'])
                            i += 1

                        print(
                            f"{p.pid} >> Jestem W-tym procesem w kolejka_winda_gora. Wysylam grupowe REL_WINDA_GORA! {pidy_do_windy}")

                        # Wysyłam REL_WINDA_GORA
                        p.clock += 1
                        p.send_to_all({"pid": pidy_do_windy, "timestamp": p.clock}, REL_WINDA_GORA)

                        # Obsluga W-tego procesu
                        p.kolejka_winda_gora = [x for x in p.kolejka_winda_gora if x['pid'] not in pidy_do_windy]
                        p.stan = "NA_DOLE"
                        p.clock += 1
                        p.send_to_all({"pid": pidy_do_windy, "timestamp": p.clock}, REL_TARAS)
                        p.kolejka_taras = [x for x in p.kolejka_taras if x['pid'] not in pidy_do_windy]
                        print(f"{p.pid} >> KOLEJKA NA TARAS: {p.kolejka_taras}")
                        p.sleep()
                        print(f'{p.pid} >> Czeka')

        elif tag == REL_WINDA_GORA:
            print(f"{p.pid} >> Dostalem REL_WINDA_GORA od {sender}")

            # Dostałem REL_WINDA_GORA
            pidy_w_windzie = msg['pid']
            p.kolejka_winda_gora = [x for x in p.kolejka_winda_gora if x['pid'] not in pidy_w_windzie]

            if p.pid in pidy_w_windzie:
                # Mój pid znajduje się w windzie
                p.stan = "NA_DOLE"

                # wait()

                # Wysyłanie REL_TARAS
                # p.clock += 1
                # p.send_to_all({"pid": p.pid, "timestamp": p.clock}, REL_TARAS)
                # p.kolejka_taras = [x for x in p.kolejka_taras if x['pid'] != p.pid]

                # Powrót do początku cyklu (punkt 2)
                p.sleep()

        elif tag == REL_TARAS:
            print(f"{p.pid} >> Dostalem REL_TARAS od {sender}")

            pidy = msg['pid']
            p.kolejka_taras = [x for x in p.kolejka_taras if x['pid'] not in pidy]
            print(f"{p.pid} >> KOLEJKA NA TARAS: {p.kolejka_taras}")