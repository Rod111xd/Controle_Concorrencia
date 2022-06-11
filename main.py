import traceback

INPUT_FILE = "in2.txt"
LOCK_TABLE_FILE = "lock_table.txt"

class Transaction:
    def __init__(self, id, ts):
        self.Id = id
        self.Ts = ts
        self.state = 'active'

class Tr_Manager:
    Tr = 0
    transactions = []
    conflicts = [] # (tr_id_1, tr_id_2); arestas de conflito

    def newTransaction(self, id):
        self.Tr += 1
        self.transactions.append(Transaction(id, self.Tr))

    def newConflict(self, tr_id_1, tr_id_2):
        if (tr_id_1, tr_id_2) not in self.conflicts:
            self.conflicts.append((tr_id_1, tr_id_2))

    
class Lock_Manager:
    # LT: [(item bloqueado, id transação, modo de bloqueio), ...]
    Lock_Table = []
    # WQ: 'item_dado': [(id transacao, modo de bloqueio), ...]
    Wait_Q = {}

    # Coloca o Lock_Table na memória
    def getLockTable(self):
        try:
            f = open(LOCK_TABLE_FILE, "r")
            content = f.read().split("\n")
            content = [l for l in content if l]
            f.close()

            for l in content:
                spl = l.split(",")
                self.Lock_Table.append((spl[0], spl[1], spl[2]))

        except FileNotFoundError:
            f = open(LOCK_TABLE_FILE, "w")
            f.write("")
            f.close()

    # Salvar e tirar Lock_Table da memória
    def saveLockTable(self):
        content = ""
        for l in self.Lock_Table:
            content += l[0] + "," + l[1] + "," + l[2] + "\n"
        
        f = open(LOCK_TABLE_FILE, "w")
        f.write(content)
        f.close()
        self.Lock_Table = []
        
    # Retorna tipo do bloqueio do dado, se estiver bloqueado retorna None
    def checkLock(self, it_data):
        self.getLockTable()

        r = None
        for lock in self.Lock_Table:
            if lock[0] == it_data:
                r = (lock[1], lock[2])
                break
        self.Lock_Table = []

        return r

    # LS(tr, D) - Solicitar bloqueio compartilhado
    def requestSharedLock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            modo_block = lock[0]
            tr_id = lock[1]
            if modo_block == 'S':
                # D.bloqueio = 'S'
                self.Lock_Table.append((it_data, 'S', tr_id))
            elif modo_block == 'X':
                # D.bloqueio = 'X'

                # Adicionar na lista da fila para o dado D
                try:
                    self.Wait_Q[it_data].append((tr_id, 'S'))
                except:
                    self.Wait_Q[it_data] = [(tr_id, 'S')]

        else:
            # D.bloqueio = 'U'
            self.Lock_Table.append((it_data, 'S', tr_id))

        self.saveLockTable()

    
    # LX(tr, D) - Solicitar bloqueio exclusivo
    def requestExclusiveLock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            modo_block = lock[0]
            tr_id = lock[1]
            if modo_block == 'S':
                # D.bloqueio = 'S'
                self.Lock_Table = [x for x in self.Lock_Table if x[0] != it_data ]
                self.Lock_Table.append((it_data, 'X', tr_id))
            elif modo_block == 'X':
                # D.bloqueio = 'X'

                # Adicionar na lista da fila para o dado D
                try:
                    self.Wait_Q[it_data].append((tr_id, 'X'))
                except:
                    self.Wait_Q[it_data] = [(tr_id, 'X')]

        else:
            # D.bloqueio = 'U'
            self.Lock_Table.append((it_data, 'X', tr_id))

        self.saveLockTable()

    # U(Tr, D) - Solicitar desbloqueio para tal dado e transação
    def requestUnlock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            self.Lock_Table = [x for x in self.Lock_Table if not (x[0] == it_data and x[2] == tr_id)]

        self.saveLockTable()


def getParamString(param):
    return param.replace("(","").replace(")","")

def readAndParseInput():
    f = open(INPUT_FILE, "r")
    lines = f.read().split("\n")

    f.close()

    ops = []

    try:
        for l in lines:
            if l[0] == 'C':
                ops.append(('C', getParamString(l[1:])))
            elif l[:2] == 'BT':
                ops.append(('BT', getParamString(l[2:])))
            elif l[0] == 'r':
                i = l.find('(')
                tr = l[1:i]
                ops.append(('r', tr, getParamString(l[i:])))
            elif l[0] == 'w':
                i = l.find('(')
                tr = l[1:i]
                ops.append(('w', tr, getParamString(l[i:])))
            else:
                raise
    except:
        print("Entrada inválida!")
        return None

    return ops

def clearLockTable():
    f = open(LOCK_TABLE_FILE, "w")
    f.write("")
    f.close()

def main():
    operations = readAndParseInput()

    if not operations:
        return

    clearLockTable()

    print(operations)

    # WAIT-DIE
    tr_manager = Tr_Manager()
    lock_manager = Lock_Manager()
    history = []

    for op in operations:
        op_type = op[0]
        tr_id = op[1]
        it_data = op[2] if len(op) > 2 else None
        
        if op_type == 'BT':
            # Começa uma nova transação
            tr_manager.newTransaction(tr_id)
            history.append(op)

        elif op_type == 'r':
            # Operação de leitura
            res = lock_manager.requestSharedLock(tr_id, it_data)


        elif op_type == 'w':
            # Operação de escrita
            res = lock_manager.requestExclusiveLock(tr_id, it_data)
        elif op_type == 'C':
            # Commit
            pass

    print("Historia: ", history)
    lock_manager.getLockTable()
    print("Lock Table: ", lock_manager.Lock_Table)
    print("Wait_Queue", lock_manager.Wait_Q)




main()

