-Data Structures:
Paxos:
peers
me: index of this paxos in peers
Instances []Instance

Instance:
sequenceNumber
isDecided
proposalNumber

AcceptorState:
highestPromiseProposalNumber: highest prepare proposal number seen
highestAcceptProposalNumber: highest accept proposal number seen
highestAcceptProposalNumberValue: value of highestAcceptProposalNumber

-Functions:
Start(sequenceNumber int, value interface{}): start agreement on instance(sequenceNumber, value):
    while not decided:
        choose proposalNumber, unique and higher than any proposalNumber seen so far
        call prepareHandler(proposalNumber) of all servers including self
        if prepare_ok(proposalNumber, highestAcceptProposalNumber, highestAcceptProposalNumberValue) from majority:
            if highestAcceptProposalNumber, highestAcceptProposalNumberValue exist:
                value' = highestAcceptProposalNumberValue;
            else:
                value' = my own value

        call acceptHandlers(proposalNumber, value) of all servers including self
        if accept_ok(proposalNumber) from majority:
            send decided(value') to all

Status(sequenceNumber int) (fate Fate, value interface{}): get info about an instance
    return the isDecided with sequenceNumber from Instances

Done(sequenceNumber int): ok to forget all instances <= sequenceNumber
	// tell each other local peer the highest done number

Max() int: highest instance sequenceNumber known, or -1

Min() int: instances before this have been forgotten

PrepareHandler(proposalNumber): handle prepare RPC calls sent by other libpaxos peers
    if proposalNumber > highestPromiseProposalNumber
        highestPromiseProposalNumber = proposalNumber
        reply prepare_ok(proposalNumber, highestAcceptProposalNumber, highestAcceptProposalNumberValue)
    else
        reply prepare_reject

AcceptHandler(proposalNumber, value): handle accept RPC calls sent by other libpaxos peers
    if proposalNumber >= highestPromiseProposalNumber
        highestPromiseProposalNumber = proposalNumber
        highestAcceptProposalNumber = proposalNumber
        highestAcceptProposalNumberValue = value
        reply accept_ok(proposalNumber)
    else
        reply accept_reject

DoneHandler(): process done RPC calls by other peers
	// Each Paxos peer will then have a Done value from each other peer. It should find the minimum, and discard all instances with sequence numbers <=that minimum.

---
ver2:
--Data Structures--:
Paxos:
peers
me: index of this paxos in peers
InstanceSlots []Instance
DoneNumberOfPaxosPeers []int

Instance:
sequenceNumber
isDecided
Acceptor

AcceptorState:
highestPromiseProposalNumber: highest prepare proposal number seen
highestAcceptProposalNumber: highest accept proposal number seen
highestAcceptProposalNumberValue: value of highestAcceptProposalNumber

--Functions--:
GenerateProposalNumber(sequenceNumber, paxosServerId): choose proposalNumber, unique and higher than any proposalNumber seen so far
    return higher 32 bit, sequenceNumber, lower 32bit paxosServerId

PrepareHandler(sequenceNumber, proposalNumber): handle prepare RPC calls sent by other libpaxos peers
    if sequenceNumber(instanceSlotIndex) is not first time seen:
        if proposalNumber > highestPromiseProposalNumber
            highestPromiseProposalNumber = proposalNumber
            reply prepare_ok(proposalNumber, highestAcceptProposalNumber, highestAcceptProposalNumberValue)
        else
            reply prepare_reject
    else:
        InstanceSlots[sequenceNumber(instanceSlotIndex)] = {sequenceNumber, notDecided, Acceptor} // Acceptor is initialized as initial values

AcceptHandler(sequenceNumber, proposalNumber, value): handle accept RPC calls sent by other libpaxos peers
    if sequenceNumber(instanceSlotIndex) is not first time seen:
        if proposalNumber >= highestPromiseProposalNumber
            highestPromiseProposalNumber = proposalNumber
            highestAcceptProposalNumber = proposalNumber
            highestAcceptProposalNumberValue = value
            reply accept_ok(proposalNumber)
        else
            reply accept_reject
    else: // This Paxos is missing out on InstanceSlots[sequenceNumber]
        InstanceSlots[sequenceNumber(instanceSlotIndex)] = {sequenceNumber, notDecided, Acceptor} // Acceptor is initialized accordingly

LearnHandler(sequenceNumber, proposalNumber, value): handle Learn RPC calls sent by other libpaxos peers
    InstanceSlots[sequenceNumber(instanceSlotIndex)] = {sequenceNumber, Decided, Acceptor} // Acceptor is initialized accordingly

DoneHandler(): process done RPC calls by other peers
	// Each Paxos peer will then have a Done value from each other peer. It should find the minimum, and discard all instances with sequence numbers <=that minimum.

Start(sequenceNumber int, value interface{}): start agreement on instance(sequenceNumber, value):
    while not decided:
        proposalNumber ++;
        call prepareHandler(proposalNumber) of all servers including self
            if prepare_ok(proposalNumber, highestAcceptProposalNumber, highestAcceptProposalNumberValue) from majority:
                if highestAcceptProposalNumber, highestAcceptProposalNumberValue exist:
                    value' = highestAcceptProposalNumberValue;
                else:
                    value' = my own value
            else:
                continue trying?

        call acceptHandlers(proposalNumber, value') of all servers including self
            if accept_ok(proposalNumber) from majority:
                call LearnHandler(sequenceNumber, proposalNumber, value) on all Paxos peers

Status(sequenceNumber int) (fate Fate, value interface{}): get info about an instance
    return InstanceSlots[sequenceNumber].isDecided

Done(sequenceNumber int): ok to forget all instances <= sequenceNumber
	// tell each other local peer the highest done number


Max() int: highest instance sequenceNumber known, or -1

Min() int: instances before this have been forgotten

---
ver3:
--Data Structures--:
Paxos:
peers []string
me int: index of this paxos in peers
InstanceSlots []Instance
DoneNumberOfPaxosPeers []int

Instance:
sequenceNumber
DecisionState
AcceptorState

AcceptorState:
highestPromiseProposalNumber: highest prepare proposal number seen
highestAcceptProposalNumber: highest accept proposal number seen
highestAcceptProposalNumberValue: value of highestAcceptProposalNumber

DecisionState:
notDecided
Decided
Forgotten(However, any live instance in InstanceSlots should not have this state.)

--Functions--:
GetProposalNumber(sequenceNumber, paxosServerId): choose proposalNumber, unique and higher than any proposalNumber seen so far
    return higher 32 bit, sequenceNumber, lower 32bit paxosServerId

PrepareHandler(sequenceNumber, proposalNumber): handle prepare RPC calls sent by other libpaxos peers
    if sequenceNumber(aka InstanceSlotsIndex) is not first time seen:
        if proposalNumber > AcceptorState.highestPromiseProposalNumber
            AcceptorState.highestPromiseProposalNumber = proposalNumber
            reply prepare_ok(AcceptorState.highestPromiseProposalNumber, AcceptorState.highestAcceptProposalNumber, AcceptorState.highestAcceptProposalNumberValue)
        else
            reply prepare_reject
    else:
        AcceptorState = {highestPromiseProposalNumber = -1, highestAcceptProposalNumber = -1, highestAcceptProposalNumberValue = null} // Acceptor is initialized as initial values
        InstanceSlots[sequenceNumber(aka InstanceSlotsIndex)] = {sequenceNumber, notDecided, AcceptorState}

AcceptHandler(sequenceNumber, proposalNumber, value): handle accept RPC calls sent by other libpaxos peers
    if sequenceNumber(aka InstanceSlotsIndex) is not first time seen:
        if proposalNumber >= AcceptorState.highestPromiseProposalNumber
            AcceptorState.highestPromiseProposalNumber = proposalNumber
            AcceptorState.highestAcceptProposalNumber = proposalNumber
            AcceptorState.highestAcceptProposalNumberValue = value
            reply accept_ok(proposalNumber)
        else
            reply accept_reject
    else: // This Paxos is missing out on InstanceSlots[sequenceNumber(aka InstanceSlotsIndex)]
        AcceptorState = {highestPromiseProposalNumber = proposalNumber, highestAcceptProposalNumber = proposalNumber, highestAcceptProposalNumberValue = value} // Acceptor is initialized accordingly
        InstanceSlots[sequenceNumber(aka InstanceSlotsIndex)] = {sequenceNumber, notDecided, AcceptorState}

LearnHandler(sequenceNumber, proposalNumber, value): handle Learn RPC calls sent by other libpaxos peers
    AcceptorState = {highestPromiseProposalNumber = proposalNumber, highestAcceptProposalNumber = proposalNumber, highestAcceptProposalNumberValue = value} // it shouldn't be changed hereafter
    InstanceSlots[sequenceNumber(aka instanceSlotIndex)] = {sequenceNumber, Decided, AcceptorState} // Acceptor is initialized accordingly

DoneHandler(sequenceNumberToForget, callerPaxosServerIndex): process done RPC calls by other peers
	// Each Paxos peer will then have a Done value from each other peer.
	// It should find the minimum, and discard all instances with sequence numbers <=that minimum.
	DoneNumberOfPaxosPeers[callerPaxosServerIndex] = sequenceNumberToForget
	minSequenceNumberToForget = Min()
	Delete all Instance that have index <= minSequenceNumberToForget from InstanceSlots

Done(sequenceNumberToForget): ok to forget all instances <= sequenceNumber
	// tell each other local peer the highest done number
	call DoneHandler(sequenceNumberToForget, px.me) of All peers

Start(sequenceNumber, value): start agreement on instance(sequenceNumber, value):
    if sequenceNumber(aka InstanceSlotsIndex) is not first time seen:
        if DecisionState = Decided:
            return
        else:
            should not happen? Because client should wait for consensus to finish
            or simply update
    else:
        AcceptorState = {highestPromiseProposalNumber = -1, highestAcceptProposalNumber = -1, highestAcceptProposalNumberValue = null} // Acceptor is initialized as initial values
        InstanceSlots[sequenceNumber(aka InstanceSlotsIndex)] = {sequenceNumber, notDecided, AcceptorState}

    proposalNumber = GetProposalNumber(sequenceNumber, px.me)
    while DecisionState != Decided and sequence >= Min():
        proposalNumber = GetProposalNumber(sequenceNumber + 1, px.me)
        call PrepareHandler(sequenceNumber, proposalNumber) of all servers including self
            if prepare_ok(proposalNumber, highestAcceptProposalNumber, highestAcceptProposalNumberValue) from majority:
                if highestAcceptProposalNumber, highestAcceptProposalNumberValue exist:
                    value' = highestAcceptProposalNumberValue;
                else:
                    value' = my own value
            else:
                continue?

        call acceptHandlers(proposalNumber, value') of all servers including self
            if accept_ok(proposalNumber) from majority:
                call LearnHandler(sequenceNumber, proposalNumber, value) on all Paxos peers
            else:
                continue?

Status(sequenceNumber) (fate Fate, value interface{}): get info about an instance
    if sequenceNumber < Min():
        return Forgotten

    if sequenceNumber(aka InstanceSlotsIndex) is not seen or InstanceSlots[sequenceNumber(aka InstanceSlotsIndex)].DecisionState == notDecided
        return notDecided
    else:
        return Decided

Max() int: highest instance sequenceNumber known, or -1

Min() int: instances before this have been forgotten
    return min(DoneNumberOfPaxosPeers) + 1