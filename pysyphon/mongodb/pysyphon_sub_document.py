import pydantic


class PysyphonSubDocument(pydantic.BaseModel):

    def psd_to_dict(self):
        return self.dict()

    @classmethod
    def psd_from_dict(cls, input_dict: dict):
        return cls(**input_dict)
