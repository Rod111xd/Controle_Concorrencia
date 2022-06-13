import traceback
from turtle import update

INPUT_FILE = "deadlock.txt"
LOCK_TABLE_FILE = "lock_table.txt"
DEBUG = False

class Transaction:
    def __init__(self, id, ts):
        self.Id = id
        self.Ts = ts
        self.state = 'active'
        self.postergadas = []

class Tr_Manager:
    Tr = 0
    transactions = {}
    conflicts = [] # (tr_id_1, tr_id_2); arestas de conflito

    def newTransaction(self, id):
        self.Tr += 1
        self.transactions[id] = Transaction(id, self.Tr)


    
class Lock_Manager:
    # LT: [(item bloqueado, id transação, modo de bloqueio), ...]
    Lock_Table = []
    # WQ: 'item_dado': [(id transacao, modo de bloqueio), ...]
    Wait_Q = {}

    def __init__(self, tr_manager):
        self.tr_manager = tr_manager

    # Coloca o Lock_Table na memória
    def getLockTable(self):
        self.Lock_Table = []

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
        
    # Retorna tipo do bloqueio do dado, se não estiver bloqueado retorna None
    def checkLock(self, it_data):
        self.getLockTable()

        r = None
        for lock in self.Lock_Table:
            if lock[0] == it_data:
                # (modo_bloqueio, tr_id)
                r = (lock[1], lock[2])
                break
        self.Lock_Table = []

        return r

    # Adicionar novo conflito na lista de conflitos
    def newConflict(self, conflict):
        inverse = (conflict[1], conflict[0])
        if inverse in self.tr_manager.conflicts:
            # Deadlock iminente
            return True
        
        if conflict not in self.tr_manager.conflicts:
            self.tr_manager.conflicts.append(conflict)
        # Conflito adicionado, não vai gerar deadlock
        return False

    # Formatar o print do grafo de conflitos
    def formatGraph(self, n_conf):
        confs = self.tr_manager.conflicts.copy()
        if n_conf not in confs:
            confs.append(n_conf)
        out = ""
        for c in confs:
            out += "    " + c[0] + "--->" + c[1]
        return out

    # LS(tr, D) - Solicitar bloqueio compartilhado
    def requestSharedLock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            modo_block = lock[0]
            tr_id_lock = lock[1]
            if modo_block == 'S':
                # D.bloqueio = 'S'
                self.Lock_Table.append((it_data, 'S', tr_id))
            elif modo_block == 'X':
                # D.bloqueio = 'X'

                # Caso a transacao T em questão já possua um LX, não gera conflito (TODO:ISSO ESTÁ CERTO?)
                if tr_id_lock != tr_id:
                    conflict = (tr_id_lock, tr_id)

                    # Adicionar conflito à lista de conflitos
                    if self.newConflict(conflict):
                        # A requisição causará um ciclo de conflitos (deadlock)
                        return 'DEADLOCK', conflict

                isTrWaiting = self.tr_manager.transactions[tr_id].state == 'wait'

                # Atualizar estado da transcao ('wait')
                self.tr_manager.transactions[tr_id].state = 'wait'

                # Adicionar na lista da fila para o dado D
                try:
                    self.Wait_Q[it_data].append((tr_id, 'S'))
                except:
                    self.Wait_Q[it_data] = [(tr_id, 'S')]

                if not isTrWaiting:
                    # Transação acabou de mudar para o estado 'wait'
                    return 'POSTERGADA', conflict
                return 'POSTERGADA', None
        else:
            # D.bloqueio = 'U'
            self.Lock_Table.append((it_data, 'S', tr_id))

        self.saveLockTable()

        return 'OK', None
   
    # LX(tr, D) - Solicitar bloqueio exclusivo
    def requestExclusiveLock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            modo_block = lock[0]
            tr_id_lock = lock[1]
            if modo_block == 'S':
                # D.bloqueio = 'S'

                # Caso a transacao T em questão já possua um LS, não gera conflito (TODO: ISSO ESTÁ CERTO?)
                if tr_id_lock != tr_id:
                    conflict = (tr_id_lock, tr_id)

                    if self.newConflict((tr_id_lock, tr_id)):
                        # A requisição causará um ciclo de conflitos (deadlock)
                        return 'DEADLOCK', conflict

                self.Lock_Table = [x for x in self.Lock_Table if x[0] != it_data ]
                self.Lock_Table.append((it_data, 'X', tr_id))
            elif modo_block == 'X':
                # D.bloqueio = 'X'

                # Caso a transacao T em questão já possua um LX, não gera conflito (TODO: ISSO ESTÁ CERTO?)
                if tr_id_lock != tr_id:
                    conflict = (tr_id_lock, tr_id)

                    if self.newConflict((tr_id_lock, tr_id)):
                        # A requisição causará um ciclo de conflitos (deadlock)
                        return 'DEADLOCK', conflict

                isTrWaiting = self.tr_manager.transactions[tr_id].state == 'wait'

                # Atualizar estado da transcao ('wait')
                self.tr_manager.transactions[tr_id].state = 'wait'

                # Adicionar na lista da fila para o dado D
                try:
                    self.Wait_Q[it_data].append((tr_id, 'X'))
                except:
                    self.Wait_Q[it_data] = [(tr_id, 'X')]

                if not isTrWaiting:
                    # Transação acabou de mudar para o estado 'wait'
                    return 'POSTERGADA', conflict
                return 'POSTERGADA', None

        else:
            # D.bloqueio = 'U'
            self.Lock_Table.append((it_data, 'X', tr_id))

        self.saveLockTable()

        return 'OK', None

    # U(Tr, D) - Solicitar desbloqueio para tal dado e transação
    def requestUnlock(self, tr_id, it_data):
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        self.getLockTable()

        if lock:
            self.Lock_Table = [x for x in self.Lock_Table if not (x[0] == it_data and x[2] == tr_id)]

        self.saveLockTable()

    def unqueue(self, item):
        if item in self.Wait_Q:
            if len(self.Wait_Q[item]) == 1:
                del self.Wait_Q[item]
                return None
            if len(self.Wait_Q[item]) > 0:
                return self.Wait_Q[item].pop(0)

        return None
    
    def removeFromLockTable(self, index):
        element = self.Lock_Table.pop(int(index))
        return element

    # REMOVER # Retorna a fila de espera para um determinado item de dado    # REMOVER 
    def getDataWaitQ(self, it_data):
        try:
            wq = self.Wait_Q[it_data]
            return wq
        except:
            pass
        return None




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

# Técnica de prevenção Wait-Die
def waitDie(tr_manager, lock_manager, operations, initialOpNumber=0):
    history = []
    response = 'OK'
    
    n_op = initialOpNumber
    
    for op in operations:
        graph = ""
        op_type = op[0]
        tr_id = op[1]
        it_data = op[2] if len(op) > 2 else None
        
        if op_type == 'BT':
            # Começa uma nova transação
            tr_manager.newTransaction(tr_id)
            history.append(op)

        elif op_type == 'r':
            # Operação de leitura

            trState = lock_manager.tr_manager.transactions[tr_id].state
            if trState == 'active':
                response, conflict = lock_manager.requestSharedLock(tr_id, it_data)
                
                if response == 'OK':
                    history.append(op)
                elif response == 'POSTERGADA':
                    graph = lock_manager.formatGraph(conflict)
                    lock_manager.tr_manager.transactions[tr_id].postergadas.append(op)
                elif response == 'DEADLOCK':
                    graph = lock_manager.formatGraph(conflict)
                    # LÓGICA DO WAITDIE
                    print("DEADLOCK iminente na operação " + str(n_op+1), "    " + graph)
                    #return history

            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                lock_manager.tr_manager.transactions[tr_id].postergadas.append(op)

        elif op_type == 'w':
            # Operação de escrita

            trState = lock_manager.tr_manager.transactions[tr_id].state
            if trState == 'active':
                response, conflict = lock_manager.requestExclusiveLock(tr_id, it_data)

                if response == 'OK':
                    history.append(op)
                elif response == 'POSTERGADA':
                    graph = lock_manager.formatGraph(conflict)
                    lock_manager.tr_manager.transactions[tr_id].postergadas.append(op)
                elif response == 'DEADLOCK':
                    graph = lock_manager.formatGraph(conflict)
                    # LÓGICA DO WAITDIE
                    print("DEADLOCK iminente na operação " + str(n_op+1), "    " + graph)
                    #return history

            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                lock_manager.tr_manager.transactions[tr_id].postergadas.append(op)

        elif op_type == 'C':

            trState = lock_manager.tr_manager.transactions[tr_id].state
            if trState == 'active':
                # Caso o estado da transação esteja ativa (sem bloqueio), executar normalmente
                lock_manager.getLockTable()

                for index, block in enumerate(lock_manager.Lock_Table):
                    if block[2] == tr_id:
                        if len(lock_manager.Lock_Table) > 0:
                            unlocked_block = lock_manager.removeFromLockTable(index)
                            unlocked_item = unlocked_block[0]
                            lock_manager.unqueue(unlocked_item)
                            lock_manager.requestUnlock(tr_id, block[0])

                lock_manager.saveLockTable()

            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                response = 'POSTERGADA'
                lock_manager.tr_manager.transactions[tr_id].postergadas.append(op)
            

        if DEBUG:
            lock_manager.getLockTable()
            print(n_op+1, ": ", op, response, graph, "      Lock Table: ", lock_manager.Lock_Table, "      Wait_Q: ", lock_manager.Wait_Q)
        else:
            print(n_op+1, ": ", op, response, graph)

        n_op += 1

    return history


def formatHistory(history):
    out = ""
    for h in history:
        if len(h) > 2:
            out += h[0] + h[1] + "(" + h[2] + ")" + "   "
        else:
            out += h[0] + "(" + h[1] + ")" + "   "
    return out

def main():
    operations = readAndParseInput()

    if not operations:
        return

    clearLockTable()

    print(operations)
    print()

    # WAIT-DIE
    tr_manager = Tr_Manager()
    lock_manager = Lock_Manager(tr_manager)
    print("Técnica WAIT-DIE")
    history = waitDie(tr_manager, lock_manager, operations.copy())
    print("\nHistoria: ", formatHistory(history))

    


main()

