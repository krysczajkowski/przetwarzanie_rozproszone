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

comm = MPI.COMM_WORLD
N = comm.Get_size()
W = 3
T = N - 1

if comm.Get_rank() == 0:
    if T <= W:
        print(f"BLAD: T ({T}) musi byc > W ({W}). Uruchom z wiecej procesami (np. mpirun -np {W + 2} python main.py)")
        comm.Abort(1)
    if N < W:
        print(f"BLAD: N ({N}) musi byc >= W ({W}).")
        comm.Abort(1)


class Process:
    def __init__(self, pid):
        self.pid = pid
        self.stan = "NA_DOLE"
        self.clock = 0

        self.kolejka_taras = []
        self.kolejka_winda_dol = []
        self.kolejka_winda_gora = []
        self.kolejka_request = []

        self.ack_taras = 0
        self.ack_winda_gora = 0
        self.ack_winda_dol = 0

        self.wakeup_time = 0
        self.is_sleeping = False

    def send_to_all(self, msg, tag):
        for dest in range(N):
            if dest != self.pid:
                comm.send(msg, dest=dest, tag=tag)

    def add_self_to_queue(self, queue, message="KOLEJKA:"):
        queue.append({'pid': self.pid, 'timestamp': self.clock})
        queue.sort(key=lambda x: (x['timestamp'], x['pid']))
        print(f'{self.pid} >> {message} {queue}')

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
        self.add_self_to_queue(queue=self.kolejka_winda_gora, message="KOLEJKA WINDA GORA:")

    def try_request_winda_dol(self):
        if self.stan != "NA_DOLE" or self.ack_taras != N - 1:
            return
        pos = self.get_index_by_pid(self.kolejka_taras, self.pid) + 1
        if pos <= 0 or pos > min(T - (T % W), N - (N % W)):
            return
        print(f"{self.pid} >> Zmieniam stan na CZEKA_NA_WINDE_DOL!")
        self.ack_winda_dol = 0
        self.ack_taras = 0
        self.stan = "CZEKA_NA_WINDE_DOL"
        self.clock += 1
        self.send_to_all({"pid": self.pid, "timestamp": self.clock}, REQ_WINDA_DOL)
        self.add_self_to_queue(self.kolejka_winda_dol, "KOLEJKA WINDA DOL:")

    def try_enter_winda_dol(self):
        if self.stan != "CZEKA_NA_WINDE_DOL" or self.ack_winda_dol != N - 1:
            return
        pos = self.get_index_by_pid(self.kolejka_winda_dol, self.pid) + 1
        print(f"{self.pid} >> Pozycja w kolejka_winda_dol {pos}")
        if pos != W:
            return
        prio_dol = (self.kolejka_winda_dol[0]['timestamp'], self.kolejka_winda_dol[0]['pid'])
        prio_gora = (float('inf'), float('inf'))
        if len(self.kolejka_winda_gora) > 0:
            prio_gora = (self.kolejka_winda_gora[0]['timestamp'], self.kolejka_winda_gora[0]['pid'])
        if prio_dol >= prio_gora:
            return
        pidy_do_windy = [self.kolejka_winda_dol[i]['pid'] for i in range(W)]
        print(f"{self.pid} >> Jestem W-tym procesem w kolejka_winda_dol. Wysylam grupowe REL_WINDA_DOL! {pidy_do_windy}")
        self.clock += 1
        self.send_to_all({"pid": pidy_do_windy, "timestamp": self.clock}, REL_WINDA_DOL)
        self.kolejka_winda_dol = [x for x in self.kolejka_winda_dol if x['pid'] not in pidy_do_windy]
        self.stan = "NA_TARASIE"
        self.sleep()
        print(f'{self.pid} >> Czeka')

    def try_enter_winda_gora(self):
        if self.stan != "CZEKA_NA_WINDE_GORA" or self.ack_winda_gora != N - 1:
            return
        pos = self.get_index_by_pid(self.kolejka_winda_gora, self.pid) + 1
        print(f"{self.pid} >> Pozycja w kolejka_winda_gora {pos}")
        if pos != W:
            return
        prio_gora = (self.kolejka_winda_gora[0]['timestamp'], self.kolejka_winda_gora[0]['pid'])
        prio_dol = (float('inf'), float('inf'))
        if len(self.kolejka_winda_dol) > 0:
            prio_dol = (self.kolejka_winda_dol[0]['timestamp'], self.kolejka_winda_dol[0]['pid'])
        if prio_gora >= prio_dol:
            return
        pidy_do_windy = [self.kolejka_winda_gora[i]['pid'] for i in range(W)]
        print(f"{self.pid} >> Jestem W-tym procesem w kolejka_winda_gora. Wysylam grupowe REL_WINDA_GORA! {pidy_do_windy}")
        self.clock += 1
        self.send_to_all({"pid": pidy_do_windy, "timestamp": self.clock}, REL_WINDA_GORA)
        self.kolejka_winda_gora = [x for x in self.kolejka_winda_gora if x['pid'] not in pidy_do_windy]
        self.stan = "NA_DOLE"
        self.clock += 1
        self.send_to_all({"pid": self.pid, "timestamp": self.clock}, REL_TARAS)
        self.kolejka_taras = [x for x in self.kolejka_taras if x['pid'] != self.pid]
        print(f"{self.pid} >> KOLEJKA NA TARAS: {self.kolejka_taras}")
        self.sleep()
        print(f'{self.pid} >> Czeka')


if __name__ == '__main__':
    p = Process(pid=comm.Get_rank())
    if p.pid == 0:
        p.NA_DOLE_wakeup()
    else:
        p.sleep()
        print(f'{p.pid} >> Czeka')

    while True:
        status = MPI.Status()
        ready = comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

        if not ready:
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

        p.clock = max(p.clock, msg['timestamp']) + 1


        if tag == REQ_TARAS:
            print(f"{p.pid} >> Dostalem REQ_TARAS od {sender}")

            index = p.get_index_by_pid(p.kolejka_taras, p.pid)
            should_defer = (
                p.stan == "NA_DOLE" and
                p.ack_taras < N - 1 and
                index != -1 and
                (msg['timestamp'], msg['pid']) > (p.kolejka_taras[index]['timestamp'], p.kolejka_taras[index]['pid'])
            )

            p.add_to_queue(p.kolejka_taras, msg, "KOLEJKA NA TARAS:")

            if should_defer:
                p.kolejka_request.append(msg)
                print(f'{p.pid} >> KOLEJKA REQUEST: {p.kolejka_request}')
            else:
                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_TARAS)


        elif tag == ACK_TARAS:
            print(f"{p.pid} >> Dostalem ACK_TARAS od {sender}")

            if p.stan == "NA_DOLE":
                p.ack_taras += 1

                pos_in_kolejka_taras = p.get_index_by_pid(p.kolejka_taras, p.pid) + 1
                print(f"{p.pid} >> Pozycja w kolejce na taras to {pos_in_kolejka_taras}")

                if p.ack_taras == N - 1:
                    while p.kolejka_request:
                        req = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=req['pid'], tag=ACK_TARAS)

                p.try_request_winda_dol()


        elif tag == REQ_WINDA_DOL:
            print(f"{p.pid} >> Dostalem REQ_WINDA_DOL od {sender}")

            index = p.get_index_by_pid(p.kolejka_winda_dol, p.pid)
            should_defer = (
                p.stan == "CZEKA_NA_WINDE_DOL" and
                p.ack_winda_dol < N - 1 and
                index != -1 and
                (msg['timestamp'], msg['pid']) > (p.kolejka_winda_dol[index]['timestamp'], p.kolejka_winda_dol[index]['pid'])
            )

            p.add_to_queue(p.kolejka_winda_dol, msg, "KOLEJKA WINDA DOL:")

            if should_defer:
                p.kolejka_request.append(msg)
                print(f'{p.pid} >> KOLEJKA REQUEST: {p.kolejka_request}')
            else:
                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_WINDA_DOL)


        elif tag == ACK_WINDA_DOL:
            print(f"{p.pid} >> Dostalem ACK_WINDA_DOL od {sender}")

            if p.stan == "CZEKA_NA_WINDE_DOL":
                p.ack_winda_dol += 1

                if p.ack_winda_dol == N - 1:
                    while p.kolejka_request:
                        req = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=req['pid'], tag=ACK_WINDA_DOL)

                p.try_enter_winda_dol()


        elif tag == REL_WINDA_DOL:
            print(f"{p.pid} >> Dostalem REL_WINDA_DOL od {sender}")

            pidy_w_windzie = msg['pid']
            p.kolejka_winda_dol = [x for x in p.kolejka_winda_dol if x['pid'] not in pidy_w_windzie]

            if p.pid in pidy_w_windzie:
                p.stan = "NA_TARASIE"
                p.ack_winda_dol = 0
                p.sleep()
                print(f'{p.pid} >> Czeka')
            else:
                p.try_enter_winda_dol()
                p.try_enter_winda_gora()


        elif tag == REQ_WINDA_GORA:
            print(f"{p.pid} >> Dostalem REQ_WINDA_GORA od {sender}")

            index = p.get_index_by_pid(p.kolejka_winda_gora, p.pid)
            should_defer = (
                p.stan == "CZEKA_NA_WINDE_GORA" and
                p.ack_winda_gora < N - 1 and
                index != -1 and
                (msg['timestamp'], msg['pid']) > (p.kolejka_winda_gora[index]['timestamp'], p.kolejka_winda_gora[index]['pid'])
            )

            p.add_to_queue(p.kolejka_winda_gora, msg, "KOLEJKA WINDA GORA:")

            if should_defer:
                p.kolejka_request.append(msg)
                print(f'{p.pid} >> KOLEJKA REQUEST: {p.kolejka_request}')
            else:
                p.clock += 1
                comm.send({"pid": p.pid, "timestamp": p.clock}, dest=sender, tag=ACK_WINDA_GORA)


        elif tag == ACK_WINDA_GORA:
            print(f"{p.pid} >> Dostalem ACK_WINDA_GORA od {sender}")

            if p.stan == "CZEKA_NA_WINDE_GORA":
                p.ack_winda_gora += 1

                if p.ack_winda_gora == N - 1:
                    while p.kolejka_request:
                        req = p.kolejka_request.pop(0)
                        p.clock += 1
                        comm.send({"pid": p.pid, "timestamp": p.clock}, dest=req['pid'], tag=ACK_WINDA_GORA)

                p.try_enter_winda_gora()


        elif tag == REL_WINDA_GORA:
            print(f"{p.pid} >> Dostalem REL_WINDA_GORA od {sender}")

            pidy_w_windzie = msg['pid']
            p.kolejka_winda_gora = [x for x in p.kolejka_winda_gora if x['pid'] not in pidy_w_windzie]

            if p.pid in pidy_w_windzie:
                p.stan = "NA_DOLE"
                p.clock += 1
                p.send_to_all({"pid": p.pid, "timestamp": p.clock}, REL_TARAS)
                p.kolejka_taras = [x for x in p.kolejka_taras if x['pid'] != p.pid]
                p.sleep()
                print(f'{p.pid} >> Czeka')
            else:
                p.try_enter_winda_dol()
                p.try_enter_winda_gora()


        elif tag == REL_TARAS:
            print(f"{p.pid} >> Dostalem REL_TARAS od {sender}")

            pid_to_remove = msg['pid']
            p.kolejka_taras = [x for x in p.kolejka_taras if x['pid'] != pid_to_remove]
            print(f"{p.pid} >> KOLEJKA NA TARAS: {p.kolejka_taras}")

            p.try_request_winda_dol()
