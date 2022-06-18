
INPUT_FILE = "deadlock.txt"
LOCK_TABLE_FILE = "lock_table.txt"
DEBUG = False

class Transaction:
    def __init__(self, id, ts):
        self.Id = id
        self.Ts = ts
        self.state = 'active'
        self.waiting = []

class Tr_Manager:
    def __init__(self):
        self.Tr = 0
        self.transactions = {}
        self.conflicts = [] # (tr_id_1, tr_id_2); tr_id_1 espera um dado bloqueado por tr_id_2
        self.curWaiting = []
        self.rollbacked = []
        self.history = []

    def newTransaction(self, id):
        self.Tr += 1
        self.transactions[id] = Transaction(id, self.Tr)


    
class Lock_Manager:

    def __init__(self, tr_manager):
        # LT: [(item bloqueado, modo de bloqueio, id transação), ...]
        self.Lock_Table = []
        # WQ: 'item_dado': [(id transacao, modo de bloqueio), ...]
        self.Wait_Q = {}
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

        r = []
        for lock in self.Lock_Table:
            if lock[0] == it_data:
                # (modo_bloqueio, tr_id)
                r.append((lock[1], lock[2]))
        
        self.saveLockTable()
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
    def formatGraph(self, novos_conf=[]):
        confs = self.tr_manager.conflicts.copy()
        for novo_conf in novos_conf:
            if novo_conf not in confs:
                if novo_conf:
                    confs.append(novo_conf)
        out = ""
        for c in confs:
            out += "    T" + c[0] + "--->T" + c[1]
        return out

    # Lógica da técnica de wait-die
    def waitDie(self, tr_id, tr_id_lock, it_data, op, conflict):
        # tr_id deseja um dado bloqueado por tr_id_lock

        if self.tr_manager.transactions[tr_id].Ts < self.tr_manager.transactions[tr_id_lock].Ts:
            # tr_id é mais velha que tr_id_lock
            # tr_id (mais velha) espera

            isTrWaiting = self.tr_manager.transactions[tr_id].state == 'wait'

            # Atualizar estado da transcao ('wait')
            self.tr_manager.transactions[tr_id].state = 'wait'

            # Adicionar na lista da fila para o dado D
            try:
                self.Wait_Q[it_data].append((tr_id, 'S'))
            except:
                self.Wait_Q[it_data] = [(tr_id, 'S')]

            # Adiciona operação na lista de espera da transação se ainda não houver
            if op not in self.tr_manager.transactions[tr_id].waiting:
                self.tr_manager.transactions[tr_id].waiting.append(op)

            # Adicionar na fila de espera de transações
            if tr_id not in self.tr_manager.curWaiting:
                self.tr_manager.curWaiting.append(tr_id)

            if not isTrWaiting:
                # Transação acabou de mudar para o estado 'wait'
                return 'POSTERGADA', conflict
            return 'POSTERGADA', None
        else:
            # tr_id é mais nova que tr_id_lock
            # tr_id (mais nova) sofre Rollback

            # Libera todos os bloqueios de tr_id
            self.getLockTable()
            self.Lock_Table = [x for x in self.Lock_Table if x[2] != tr_id ]
            self.saveLockTable()

            # Guarda a informação de que tr_id teve Rollback
            #self.tr_manager.curWaiting.append(tr_id)
            self.tr_manager.rollbacked.append(tr_id)
            self.tr_manager.transactions[tr_id].state = 'wait'
            
            # Remover conflitos com a transação que teve rollback
            self.tr_manager.conflicts = [x for x in self.tr_manager.conflicts if x[1] != tr_id]
        
            # Sinalizar que tr_id_lock deve ser executada
            self.tr_manager.transactions[tr_id_lock].state = 'active'

            # Remove operações bem sucedidas da história e adiciona na lista de espera da transação tr_id
            self.tr_manager.transactions[tr_id].waiting = [x for x in self.tr_manager.history if x[1] == tr_id]
            self.tr_manager.history = [x for x in self.tr_manager.history if x[1] != tr_id]

            # Adiciona na lista de espera da transação tr_id a operação que causou o rollback
            if op not in self.tr_manager.transactions[tr_id].waiting:
                self.tr_manager.transactions[tr_id].waiting.append(op)

            return 'ROLLBACK', "T" + tr_id + " ROLLBACKED"
        
    # Lógica da técnica de wound-wait
    def woundWait(self, tr_id, tr_id_lock, it_data, op, conflict):
        # tr_id deseja um dado bloqueado por tr_id_lock

        if self.tr_manager.transactions[tr_id].Ts < self.tr_manager.transactions[tr_id_lock].Ts:
            # tr_id é mais velha que tr_id_lock
            # tr_id_lock (mais nova) sofre Rollback

            # Libera todos os bloqueios de tr_id_lock
            self.getLockTable()
            self.Lock_Table = [x for x in self.Lock_Table if x[2] != tr_id_lock ]
            self.saveLockTable()

            # Guarda a informação de que tr_id_lock teve Rollback
            #self.tr_manager.curWaiting.append(tr_id_lock)
            self.tr_manager.rollbacked.append(tr_id_lock)
            self.tr_manager.transactions[tr_id_lock].state = 'wait'

            # Remover conflitos com a transação que teve rollback
            self.tr_manager.conflicts = [x for x in self.tr_manager.conflicts if x[1] != tr_id_lock]

            # Sinalizar que tr_id deve ser executada
            self.tr_manager.transactions[tr_id].state = 'active'

            # Remove operações bem sucedidas da história e adiciona na lista de espera da transação tr_id_lock
            self.tr_manager.transactions[tr_id_lock].waiting = [x for x in self.tr_manager.history if x[1] == tr_id_lock]
            self.tr_manager.history = [x for x in self.tr_manager.history if x[1] != tr_id_lock]

            return 'ROLLBACK2', "T" + tr_id_lock + " ROLLBACKED"

        else:
            # tr_id é mais nova que tr_id_lock
            # tr_id (mais nova) espera

            isTrWaiting = self.tr_manager.transactions[tr_id].state == 'wait'

            # Atualizar estado da transcao ('wait')
            self.tr_manager.transactions[tr_id].state = 'wait'

            # Adicionar na lista da fila para o dado D
            try:
                self.Wait_Q[it_data].append((tr_id, 'S'))
            except:
                self.Wait_Q[it_data] = [(tr_id, 'S')]

            # Adiciona operação na lista de espera da transação se ainda não houver
            if op not in self.tr_manager.transactions[tr_id].waiting:
                self.tr_manager.transactions[tr_id].waiting.append(op)

            # Adicionar na fila de espera de transações
            if tr_id not in self.tr_manager.curWaiting:
                self.tr_manager.curWaiting.append(tr_id)

            if not isTrWaiting:
                # Transação acabou de mudar para o estado 'wait'
                return 'POSTERGADA', conflict
            return 'POSTERGADA', None


    # LS(tr, D) - Solicitar bloqueio compartilhado
    def requestSharedLock(self, op, method, n_op=0):
        tr_id = op[1]
        it_data = op[2] 

        extra = ""
        
        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        #print(op, lock)
        if lock:        
            modo_block = 'S' if len(lock) > 1 else lock[0][0]
            tr_id_lock = lock[0][1]
            if modo_block == 'S':
                # D.bloqueio = 'S'
                self.getLockTable()
                self.Lock_Table.append((it_data, 'S', tr_id))
                self.saveLockTable()
            elif modo_block == 'X':
                # D.bloqueio = 'X'

                # Caso a transacao T em questão já possua um LX, não gera conflito (TODO:ISSO ESTÁ CERTO?)
                conflict = ""
                if tr_id_lock != tr_id:
                    conflict = (tr_id, tr_id_lock)

                    # Adicionar conflito à lista de conflitos
                    if self.newConflict(conflict):
                        # A requisição causará um ciclo de conflitos (deadlock)
                        graph = self.formatGraph([conflict])
                        extra += "DEADLOCK iminente na operação " + str(n_op+1), "    " + graph + "\n"
                        
                    if method == 'wait-die':
                        r1, r2 = self.waitDie(tr_id, tr_id_lock, it_data, op, conflict)
                        return r1, extra + r2

                    elif method == 'wound-wait':
                        r1, r2 = self.woundWait(tr_id, tr_id_lock, it_data, op, conflict)
                        return r1, extra + r2

        else:
            # D.bloqueio = 'U'
            self.getLockTable()
            self.Lock_Table.append((it_data, 'S', tr_id))
            self.saveLockTable()

        return 'OK', None
   
    # LX(tr, D) - Solicitar bloqueio exclusivo
    def requestExclusiveLock(self, op, method, n_op=0):
        tr_id = op[1]
        it_data = op[2]

        extra = ""

        # lock = (modo bloqueio, tr_id) ou None(sem bloqueio)
        lock = self.checkLock(it_data)

        #print(op, lock)
        if lock:
            modo_block = 'S' if len(lock) > 1 else lock[0][0]

            if modo_block == 'S':
                # D.bloqueio = 'S'
                ret1 = ""
                ret2 = []
                diff = False

                if len(lock) == 1 and lock[0][1] == tr_id:
                    self.getLockTable()
                    self.Lock_Table = [x for x in self.Lock_Table if x[0] != it_data ]
                    self.Lock_Table.append((it_data, 'X', tr_id))
                    self.saveLockTable()
                else:
                    for lk in lock:
                        tr_id_lock = lk[1]

                        # Caso a transacao T em questão já possua um LS, não gera conflito (TODO: ISSO ESTÁ CERTO?)
                        conflict = ""
                        if tr_id_lock != tr_id:
                            
                            diff = True
                            conflict = (tr_id, tr_id_lock)

                            if self.newConflict(conflict):
                                # A requisição causará um ciclo de conflitos (deadlock)
                                graph = self.formatGraph([conflict])
                                extra += "DEADLOCK iminente na operação " + str(n_op+1) + "    " + graph + "\n"

                            if method == 'wait-die':
                                r1, r2 = self.waitDie(tr_id, tr_id_lock, it_data, op, conflict)
                                ret1 = r1
                                if r1 == 'ROLLBACK' or r1 == 'ROLLBACK2':
                                    ret2.append(extra + r2)
                                else:
                                    ret2.append(r2)

                            elif method == 'wound-wait':
                                r1, r2 = self.woundWait(tr_id, tr_id_lock, it_data, op, conflict)
                                ret1 = r1
                                if r1 == 'ROLLBACK' or r1 == 'ROLLBACK2':
                                    ret2.append(extra + r2)
                                else:
                                    ret2.append(r2)
                                
                       
                if diff:
                    return ret1, ret2
            elif modo_block == 'X':
                # D.bloqueio = 'X'
                tr_id_lock = lock[0][1]

                # Caso a transacao T em questão já possua um LX, não gera conflito (TODO: ISSO ESTÁ CERTO?)
                conflict = ""
                if tr_id_lock != tr_id:
                    conflict = (tr_id, tr_id_lock)

                    if self.newConflict(conflict):
                        # A requisição causará um ciclo de conflitos (deadlock)
                        graph = self.formatGraph([conflict])
                        extra += "DEADLOCK iminente na operação " + str(n_op+1) + "    " + graph + "\n"
                        

                if method == 'wait-die':
                    r1, r2 = self.waitDie(tr_id, tr_id_lock, it_data, op, conflict)
                    return r1, [extra + r2] if r1 == 'ROLLBACK' or r1 == 'ROLLBACK2' else [r2]

                elif method == 'wound-wait':
                    r1, r2 = self.woundWait(tr_id, tr_id_lock, it_data, op, conflict)
                    return r1, [extra + r2] if r1 == 'ROLLBACK' or r1 == 'ROLLBACK2' else [r2]

        else:
            # D.bloqueio = 'U'
            self.getLockTable()
            self.Lock_Table.append((it_data, 'X', tr_id))
            self.saveLockTable()

        return 'OK', None

    # U(Tr, D) - Solicitar desbloqueio para tal dado e transação TODO: FAZER ALGO COM O METODO
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
        self.getLockTable()
        element = self.Lock_Table.pop(int(index))
        self.saveLockTable()
        return element



# Remover parênteses
def getParamString(param):
    return param.replace("(","").replace(")","")

# Ler entrada e retornar operações
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

# Limpar Lock_Table do disco
def clearLockTable():
    f = open(LOCK_TABLE_FILE, "w")
    f.write("")
    f.close()

def executeWaiting(lock_manager, method):
    for tr in lock_manager.tr_manager.curWaiting:
        # Se a transação ainda estiver em conflito, não executar
        for conf in lock_manager.tr_manager.conflicts:
            if conf[0] == tr:
                continue
        
        # Transação agora volta a estar ativa sem espera
        lock_manager.tr_manager.transactions[tr].state = 'active'

        # Remover da lista de transações em espera
        lock_manager.tr_manager.curWaiting = [x for x in lock_manager.tr_manager.curWaiting if x != tr]

        # Recuperar lista de operações em espera de tr
        waitingList = lock_manager.tr_manager.transactions[tr].waiting
        
        # Esvaziar lista de operações em espera de tr
        lock_manager.tr_manager.transactions[tr].waiting = []
        
        # Executar operações em espera
        execute(lock_manager.tr_manager, lock_manager, waitingList, method, label='E')

def executeRollbacked(lock_manager, method):
    for tr in lock_manager.tr_manager.rollbacked:
        # Se a transação ainda estiver em conflito, não executar
        for conf in lock_manager.tr_manager.conflicts:
            if conf[0] == tr:
                continue
        
        # Transação agora volta a estar ativa sem espera
        lock_manager.tr_manager.transactions[tr].state = 'active'

        # Remover da lista de transações rollbacked
        lock_manager.tr_manager.rollbacked = [x for x in lock_manager.tr_manager.rollbacked if x != tr]

        # Remover operação de criar transação (Usar mesmo Timestamp)
        lock_manager.tr_manager.transactions[tr].waiting = [x for x in lock_manager.tr_manager.transactions[tr].waiting if x[0] != 'BT']

        # Recuperar lista de operações em espera de tr
        waitingList = lock_manager.tr_manager.transactions[tr].waiting

        # Esvaziar lista de operações em espera de tr
        lock_manager.tr_manager.transactions[tr].waiting = []
        
        # Executar operações em espera
        execute(lock_manager.tr_manager, lock_manager, waitingList, method, label='R')

def printOperation(lock_manager, op, n_op, tr_id, label, response):
    lb = str(n_op+1) if label is None else label + "T" + tr_id
    if DEBUG:
        lock_manager.getLockTable()
        print(lb + ": ", formatHistory([op]), response, lock_manager.formatGraph(), "      Lock Table: ", lock_manager.Lock_Table, "      Wait_Q: ", lock_manager.Wait_Q)
        lock_manager.saveLockTable()
    else:
        print(lb + ": ", formatHistory([op]), response, lock_manager.formatGraph())
        
# Técnica de prevenção Wait-Die
def execute(tr_manager, lock_manager, operations, method, label=None):
    
    for n_op, op in enumerate(operations):
        response = 'OK'
        graph = ""
        op_type = op[0]
        tr_id = op[1]
        it_data = op[2] if len(op) > 2 else None
        
        if op_type == 'BT':
            # Começa uma nova transação
            tr_manager.newTransaction(tr_id)
            tr_manager.history.append(op)

        elif op_type == 'r':
            # Operação de leitura

            trState = lock_manager.tr_manager.transactions[tr_id].state
            if trState == 'active':
                response, extra = lock_manager.requestSharedLock(op, method, n_op=n_op)
                
                if response == 'OK':
                    tr_manager.history.append(op)
                elif response == 'POSTERGADA':
                    pass
                    #graph = lock_manager.formatGraph(extra)
                elif response == 'ROLLBACK' or response == 'ROLLBACK2':
                    # Saída antecipada da operação
                    response = 'OK' if response == 'ROLLBACK2' else response
                    if response == 'OK':
                        tr_manager.history.append(op)
                    printOperation(lock_manager, op, n_op, tr_id, label, response)
                    if extra != "":
                        print(extra)
                    # Tentar executar transações em espera
                    executeWaiting(lock_manager, method)

                    continue
            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                response = 'POSTERGADA'
                lock_manager.tr_manager.transactions[tr_id].waiting.append(op)

        elif op_type == 'w':
            # Operação de escrita
            
            trState = lock_manager.tr_manager.transactions[tr_id].state
            
            if trState == 'active':
                
                response, extra = lock_manager.requestExclusiveLock(op, method, n_op=n_op)

                if response == 'OK':
                    tr_manager.history.append(op)
                elif response == 'POSTERGADA':
                    graph = lock_manager.formatGraph(extra)
                elif response == 'ROLLBACK' or response == 'ROLLBACK2':
                    # Saída antecipada da operação
                    response = 'OK' if response == 'ROLLBACK2' else response
                    if response == 'OK':
                        tr_manager.history.append(op)
                    printOperation(lock_manager, op, n_op, tr_id, label, response)
                    if extra:
                        print("\n".join(extra))
                    # Tentar executar transações em espera
                    executeWaiting(lock_manager, method)
                    continue
            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                response = 'POSTERGADA'
                
                lock_manager.tr_manager.transactions[tr_id].waiting.append(op)

        elif op_type == 'C':
            
            trState = lock_manager.tr_manager.transactions[tr_id].state
            if trState == 'active':
                tr_manager.history.append(op)

                # Caso o estado da transação esteja ativa (sem bloqueio), executar normalmente
                lock_manager.getLockTable()
                lock_manager.Lock_Table = [x for x in lock_manager.Lock_Table if x[2] != tr_id]
                lock_manager.saveLockTable()

                # Reativar transações que estavam em conflito por causa de tr_id
                trsToActive = [x[0] for x in lock_manager.tr_manager.conflicts if x[1] == tr_id]
                for trActive in trsToActive:
                    tr_manager.transactions[trActive].state = 'active'

                # Remover conflitos de origem em tr_id
                lock_manager.tr_manager.conflicts = [x for x in lock_manager.tr_manager.conflicts if x[1] != tr_id]

                # Alternar tr_id para estado finalizado
                lock_manager.tr_manager.transactions[tr_id].state = 'done'

                # Saída antecipada da operação
                printOperation(lock_manager, op, n_op, tr_id, label, response)

                # Tentar executar transações em espera
                executeWaiting(lock_manager, method)

                # Tentar reiniciar transações rollbacked
                executeRollbacked(lock_manager, method)
                continue
            elif trState == 'wait':
                # Caso o estado da transação esteja em espera, postergar
                response = 'POSTERGADA'
                lock_manager.tr_manager.transactions[tr_id].waiting.append(op)
        
        printOperation(lock_manager, op, n_op, tr_id, label, response)

        
def formatOperations(operations):
    for i, op in enumerate(operations):
        if len(op) > 2:
            print(str(i+1) + ": " + op[0] + op[1] + "(" + op[2] + ")")
        else:
            print(str(i+1) + ": " + op[0] + "(" + op[1] + ")")

def formatHistory(history):
    out = ""
    for h in history:
        if h[0] == 'BT':
            continue
        if len(h) > 2:
            out += h[0] + h[1] + "(" + h[2] + ")" + "   "
        else:
            out += h[0] + "(" + h[1] + ")" + "   "
            if h[0] == 'C':
                out += " "
    return out

def main():
    operations = readAndParseInput()

    if not operations:
        return

    print("Entrada:")
    formatOperations(operations)

    # WAIT-DIE
    clearLockTable()
    tr_manager = Tr_Manager()
    lock_manager = Lock_Manager(tr_manager)
    print("\nTécnica WAIT-DIE")
    execute(tr_manager, lock_manager, operations.copy(), 'wait-die')
    print("História: ", formatHistory(tr_manager.history))

    # WOUND-WAIT
    clearLockTable()
    tr_manager = Tr_Manager()
    lock_manager = Lock_Manager(tr_manager)
    print("\nTécnica WOUND-WAIT")
    execute(tr_manager, lock_manager, operations.copy(), 'wound-wait')
    print("História: ", formatHistory(tr_manager.history))



main()

