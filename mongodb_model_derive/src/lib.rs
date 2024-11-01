use darling::FromDeriveInput;
use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[derive(FromDeriveInput, Default)]
#[darling(default, attributes(mongo_model))]
struct Opts {
    collection: String,
    id_field: Option<String>,
}

#[proc_macro_derive(MongoModel, attributes(mongo_model))]
pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);
    let opts = Opts::from_derive_input(&input).expect("Wrong options");
    let DeriveInput { ident, .. } = input;
    let collection = opts.collection;

    let const_collection = quote! {
        const COLLECTION_NAME: &'static str = #collection;
    };

    // TODO: Support overriding the _id / id field mapping here
    let fn_id = quote! {
        fn id(&self) -> Option<::bson::oid::ObjectId> {
            self.id
        }
    };

    // TODO: Support overriding the _id / id field mapping here
    let fn_set_id = quote! {
        fn set_id(&mut self, new_id: ::bson::oid::ObjectId) {
            self.id = Some(new_id);
        }
    };

    let output = quote! {

        // impl ::mongodb_model::MongoModel for #ident {
         impl ::tuxedo::mongodb_model::MongoModel for #ident {
            #const_collection
            #fn_id
            #fn_set_id
        }
    };

    output.into()
}
