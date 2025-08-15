from dataclasses import asdict, dataclass, field, fields
from datetime import datetime


@dataclass
class CustomerData():
    _id: int
    name: str
    email: str = field(default=None)
    phone: str = field(default=None)
    country: str = field(default=None)
    signup_date: datetime = field(default=None)

    @staticmethod
    def fields():
        return [f.name for f in fields(CustomerData)]

    def to_dict(self):
        return asdict(self)

    def __post_init__(self):
        if isinstance(self.signup_date, str):
            self.signup_date = datetime.fromisoformat(self.signup_date)


if __name__ == '__main__':
    customer_fields = CustomerData.fields()
    print(customer_fields)
    customer = CustomerData(_id=1,
                            name='test',
                            country='France')
    print(customer.__getattribute__('country'))
