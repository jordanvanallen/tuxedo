use fake::Fake;
use fake::{
    faker::{
        address::en::{PostCode, StreetName},
        internet::en::SafeEmail,
        lorem::en::Sentence,
        name::en::{FirstName, LastName, Name},
        number::en::Digit,
        phone_number::en::PhoneNumber,
    },
    // locales::EN,
};
// use rand::rngs::StdRng;
// use rand::SeedableRng;
// TODO: I would like to try and be able to have recreatable
// data between runs by using the client_id or denote some field
// like _id that's always present to be the SEED for Fake
// this might give us the results we want?
// this would have to happen in the mask trait

pub trait Mask {
    fn mask(&mut self);

    fn seed() -> i32 {
        12345
    }

    /// Provides the ability to fake a person's name.
    fn fake_name() -> String {
        Name().fake()
    }

    /// Provides the ability to fake a person's first name.
    fn fake_first_name() -> String {
        FirstName().fake()
    }

    /// Provides the ability to fake a person's last name.
    fn fake_last_name() -> String {
        LastName().fake()
    }

    // Provides the ability to fake a person's full name (first + last)
    fn fake_full_name() -> String {
        let mut name: String = Self::fake_last_name();
        let first_name: &str = &Self::fake_first_name();

        name.push_str(", ");
        name.push_str(first_name);
        name
    }

    /// Provides the ability to fake some comments or sample text.
    fn fake_comments() -> String {
        Sentence(1..3).fake()
    }

    /// Provides the ability to fake an email address.
    fn fake_email() -> String {
        SafeEmail().fake()
    }

    /// Provides the ability to fake a street name / address.
    fn fake_address() -> String {
        StreetName().fake()
    }

    /// Provides the ability to fake a Canadian postal code.
    fn fake_postal_code() -> String {
        PostCode().fake()
    }

    /// Provides the ability to fake a phone number.
    fn fake_phone_number() -> String {
        PhoneNumber().fake()
    }

    fn fake_phone_number_extension() -> String {
        Self::fake_numeric_string(3)
    }

    /// Provides the ability to fake a numeric string of a given length.
    fn fake_numeric_string(length: usize) -> String {
        let mut list: Vec<String> = Vec::with_capacity(length);
        for _ in 1..length {
            list.push(Digit().fake::<String>());
        }

        list.concat()
    }
}

