

class SpecificDisciplineParentMismatch(Exception):
    """
    This exception is caused when we're attempting to match up duplicate specific disciplines from
    the 4 synth databases. We attempt to de-duplicate them by matching the name and this exception
    simply indicates that there was a name match between two synth databases but that the parent
    discipline of the specific disciplines that matched were different.
    """

    def __init__(self, synth_round, specific_discipline_id, new_displine_id, added_discipline_id):
        super().__init__(f"Specific discipline {specific_discipline_id} in synth round "
                         f"{synth_round.value} has the same name as another specific discipline in "
                         f"another synth round that we've already added but has a different "
                         f"parent discipline ID: {new_displine_id} != {added_discipline_id}")
        self.synth_round = synth_round
        self.specific_discipline_id = specific_discipline_id
        self.new_displine_id = new_displine_id
        self.added_discipline_id = added_discipline_id
