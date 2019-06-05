import json
import pickle


def serialize_action_result(action):
    def serializer(*args):
        action_name, hits = action(*args)
    return


def serialize_json(*args, task_configs):
    pass


def deserialize_json(*args, **kwargs):
    pass

    # submission_type = 'production_' if self.in_production else 'sandbox_'

    # if 'hit_filename' in kwargs:
    #     serialize(hits_created, filename=kwargs['hit_filename'], timestamp=False)
    # else:
    #     serialize(
    #         hits_created, f'submitted_batch_{submission_type + str(len(hits_created))}', timestamp=True)
