// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use ::syn::{
    Token,
    Type,
    TypePath,
    parse::{
        self,
        Parse,
        ParseStream,
    },
};

use ::proc_macro2::Ident;

/// Helper struct for parsing the `load!` macro input
pub struct LoadInput {
    pub lua_expr: Ident,
    pub type_paths: Vec<TypePath>,
}

/// Custom parser for `lua, MyStruct, MyEnum, ...`
impl Parse for LoadInput {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let lua_expr: Ident = input.parse()?;
        let mut type_paths = Vec::new();

        // Continue parsing idents as long as there's a comma
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            } // Allow trailing comma

            let ty: Type = input.parse()?;
            match ty {
                Type::Path(type_path) => type_paths.push(type_path),
                other => {
                    return Err(syn::Error::new_spanned(other, "expected a type path"));
                }
            }
        }

        Ok(Self {
            lua_expr,
            type_paths,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::LoadInput;

    #[test]
    fn load_rejects_non_path_types() {
        let error = match syn::parse2::<LoadInput>(quote::quote!(lua, &str)) {
            Ok(_) => panic!("expected non-path type to be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.to_string(), "expected a type path");
    }
}
