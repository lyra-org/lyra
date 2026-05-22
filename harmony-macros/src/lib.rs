// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{
    Span,
    TokenStream as TokenStream2,
};
use quote::quote;
use syn::{
    Attribute,
    Fields,
    FnArg,
    Ident,
    ImplItem,
    Item,
    ItemEnum,
    ItemImpl,
    ItemStruct,
    LitStr,
    Meta,
    Pat,
    ReturnType,
    Token,
    Type,
    TypePath,
    TypeReference,
    parse::{
        Parse,
        ParseStream,
    },
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
};

#[derive(Default)]
struct UserDataOptions {
    name: Option<LitStr>,
    description: Option<LitStr>,
}

struct UserDataOptionList(Punctuated<UserDataOption, Token![,]>);

enum UserDataOption {
    Name(LitStr),
    Description(LitStr),
}

struct HarmonyMethodOptions {
    skip: bool,
    args: Option<Vec<DocParameterOverride>>,
    returns: Option<Vec<Type>>,
    description: Option<LitStr>,
}

struct DocParameterOverride {
    name: String,
    ty: Type,
}

struct DocParameterList(Punctuated<DocParameter, Token![,]>);

struct DocParameter {
    name: Ident,
    _colon: Token![:],
    ty: Type,
}

struct DocReturnList(Punctuated<Type, Token![,]>);

enum HarmonyMethodOption {
    Skip,
    Args(DocParameterList),
    Returns(DocReturnList),
    Description(LitStr),
}

struct HarmonyMethodOptionList(Punctuated<HarmonyMethodOption, Token![,]>);

struct MethodParameter<'a> {
    ident: &'a Ident,
    ty: &'a Type,
    name_lit: LitStr,
}

impl Parse for UserDataOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for UserDataOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        let _: Token![=] = input.parse()?;
        let value: LitStr = input.parse()?;

        if ident == "name" {
            return Ok(Self::Name(value));
        }
        if ident == "description" {
            return Ok(Self::Description(value));
        }

        Err(syn::Error::new(
            ident.span(),
            "unsupported userdata option; expected name or description",
        ))
    }
}

impl Parse for DocParameter {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self {
            name: input.parse()?,
            _colon: input.parse()?,
            ty: input.parse()?,
        })
    }
}

impl Parse for DocParameterList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for DocReturnList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

impl Parse for HarmonyMethodOption {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;

        if ident == "skip" {
            return Ok(Self::Skip);
        }

        if ident == "args" {
            let content;
            syn::parenthesized!(content in input);
            return Ok(Self::Args(content.parse()?));
        }

        if ident == "returns" {
            let content;
            syn::parenthesized!(content in input);
            return Ok(Self::Returns(content.parse()?));
        }

        if ident == "description" {
            let _: Token![=] = input.parse()?;
            return Ok(Self::Description(input.parse()?));
        }

        Err(syn::Error::new(
            ident.span(),
            "unsupported harmony option; expected skip, args, returns, or description",
        ))
    }
}

impl Parse for HarmonyMethodOptionList {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        Ok(Self(Punctuated::parse_terminated(input)?))
    }
}

#[proc_macro_attribute]
pub fn userdata(attr: TokenStream, item: TokenStream) -> TokenStream {
    let options = match parse_userdata_options(attr) {
        Ok(options) => options,
        Err(error) => return error.into_compile_error().into(),
    };
    let ast = parse_macro_input!(item as Item);

    match ast {
        Item::Struct(ast) => userdata_struct(ast, options).into(),
        Item::Enum(ast) => match userdata_enum(ast, options) {
            Ok(tokens) => tokens.into(),
            Err(error) => error.into_compile_error().into(),
        },
        item => syn::Error::new(
            item.span(),
            "userdata must be used on a struct or unit-variant enum",
        )
        .into_compile_error()
        .into(),
    }
}

fn userdata_struct(ast: ItemStruct, options: UserDataOptions) -> TokenStream2 {
    let type_name = &ast.ident;
    let type_name_lit = options
        .name
        .unwrap_or_else(|| lit_str(&type_name.to_string(), type_name.span()));
    let description_expr = option_lit_str_tokens(options.description.as_ref());
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    quote! {
        #ast

        #[automatically_derived]
        impl #impl_generics ::harmony_core::UserDataType for #type_name #ty_generics #where_clause {
            const CLASS_NAME: &'static str = #type_name_lit;
            const DESCRIPTION: ::std::option::Option<&'static str> = #description_expr;
        }

        #[automatically_derived]
        impl #impl_generics ::harmony_luau::LuauTypeInfo for #type_name #ty_generics #where_clause {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(<Self as ::harmony_core::UserDataType>::CLASS_NAME)
            }
        }
    }
}

fn userdata_enum(ast: ItemEnum, options: UserDataOptions) -> syn::Result<TokenStream2> {
    let type_name = &ast.ident;
    if !ast.generics.params.is_empty() || ast.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            ast.generics.span(),
            "userdata enums cannot be generic",
        ));
    }

    let mut variant_names = Vec::new();
    let mut variant_values = Vec::new();
    let mut field_descriptors = Vec::new();
    for variant in &ast.variants {
        if !matches!(&variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "userdata enum variants must be unit variants",
            ));
        }

        let variant_name = &variant.ident;
        let variant_name_lit = lit_str(&variant_name.to_string(), variant_name.span());
        variant_names.push(variant_name_lit.clone());
        variant_values.push(quote! { #type_name::#variant_name });
        field_descriptors.push(quote! {
            ::harmony_luau::FieldDescriptor {
                name: #variant_name_lit,
                ty: <Self as ::harmony_luau::LuauTypeInfo>::luau_type(),
                description: None,
            }
        });
    }

    let type_name_lit = options
        .name
        .unwrap_or_else(|| lit_str(&type_name.to_string(), type_name.span()));
    let description_expr = option_lit_str_tokens(options.description.as_ref());

    Ok(quote! {
        #ast

        #[automatically_derived]
        impl ::harmony_core::UserDataType for #type_name {
            const CLASS_NAME: &'static str = #type_name_lit;
            const DESCRIPTION: ::std::option::Option<&'static str> = #description_expr;
        }

        #[automatically_derived]
        impl ::harmony_luau::LuauTypeInfo for #type_name {
            fn luau_type() -> ::harmony_luau::LuauType {
                ::harmony_luau::LuauType::literal(<Self as ::harmony_core::UserDataType>::CLASS_NAME)
            }
        }

        #[automatically_derived]
        impl #type_name {
            #[doc(hidden)]
            pub fn _harmony_userdata_class() -> &'static ::harmony_core::UserDataClass<Self> {
                static CLASS: ::std::sync::LazyLock<::harmony_core::UserDataClass<#type_name>> =
                    ::std::sync::LazyLock::new(|| {
                        ::harmony_core::UserDataClass::new(
                            <#type_name as ::harmony_core::UserDataType>::CLASS_NAME,
                        )
                            .equality()
                            #(.variant(#variant_names, #variant_values))*
                    });
                &CLASS
            }

            #[doc(hidden)]
            pub fn _harmony_userdata_spec() -> ::harmony_core::UserDataSpec {
                ::harmony_core::UserDataSpec::new(
                    <#type_name as ::harmony_core::UserDataType>::CLASS_NAME,
                )
                    .initializer(|vm, origin, root| {
                        #type_name::_harmony_userdata_class().install_variant_table(vm, origin, root)
                    })
            }
        }

        #[automatically_derived]
        impl<'vm> ::harmony_luau::FromLuau<'vm> for #type_name {
            fn read(reader: &mut ::harmony_luau::ArgReader<'vm>) -> ::harmony_luau::runtime::Result<Self> {
                let userdata = <::harmony_luau::UserData as ::harmony_luau::FromLuau<'vm>>::read(reader)?;
                Self::_harmony_userdata_class().read_userdata(reader.vm(), &userdata)
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeUserData for #type_name {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                let mut descriptor = ::harmony_luau::ClassDescriptor::new(
                    <Self as ::harmony_core::UserDataType>::CLASS_NAME,
                    <Self as ::harmony_core::UserDataType>::DESCRIPTION,
                );
                descriptor.fields.extend(vec![#(#field_descriptors),*]);
                descriptor
            }
        }
    })
}

#[proc_macro_attribute]
pub fn userdata_methods(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut ast = parse_macro_input!(item as ItemImpl);
    if ast.trait_.is_some() {
        return syn::Error::new(
            ast.impl_token.span,
            "userdata_methods must be used on an inherent impl",
        )
        .into_compile_error()
        .into();
    }

    let self_ty = ast.self_ty.clone();
    let mut class_methods = Vec::new();
    let mut method_descriptors = Vec::new();
    let mut spec_methods = Vec::new();

    for item in &mut ast.items {
        let ImplItem::Fn(fn_item) = item else {
            continue;
        };

        let options = match parse_harmony_method_options(&fn_item.attrs) {
            Ok(options) => options,
            Err(error) => return error.into_compile_error().into(),
        };
        strip_helper_attributes(&mut fn_item.attrs);

        if options.skip {
            continue;
        }

        let receiver = match fn_item.sig.receiver() {
            Some(receiver) => receiver,
            None => {
                return syn::Error::new(
                    fn_item.sig.ident.span(),
                    "userdata methods must take self",
                )
                .into_compile_error()
                .into();
            }
        };
        if receiver.mutability.is_some() {
            return syn::Error::new(
                receiver.span(),
                "mutable userdata receivers are not supported; use interior mutability",
            )
            .into_compile_error()
            .into();
        }

        let fn_name = &fn_item.sig.ident;
        let fn_name_lit = lit_str(&fn_name.to_string(), fn_name.span());
        let description = option_lit_str_tokens(options.description.as_ref());
        let is_async = fn_item.sig.asyncness.is_some();
        let returns_future = returns_scheduled_future(&fn_item.sig.output);

        if is_async && has_hidden_context_parameter(&fn_item.sig.inputs) {
            return syn::Error::new(
                fn_item.sig.ident.span(),
                "async userdata methods cannot take borrowed Luau context; convert Luau values before creating the future",
            )
            .into_compile_error()
            .into();
        }

        let params = collect_parameters(&fn_item.sig.inputs);
        let read_args = params.iter().map(|param| {
            let ident = param.ident;
            let ty = param.ty;
            let name = &param.name_lit;
            quote! {
                let #ident: #ty = frame.args.read_named(#name)?;
            }
        });
        let call_args = collect_call_arguments(&fn_item.sig.inputs);
        let method_argument_names = std::iter::once(lit_str("self", fn_name.span()))
            .chain(params.iter().map(|param| param.name_lit.clone()))
            .collect::<Vec<_>>();
        let registration = if is_async {
            let body = async_method_body(fn_name, &call_args, &fn_item.sig.output);
            quote! {
                .async_method(#fn_name_lit, [#(#method_argument_names),*], |__harmony_this: #self_ty, mut frame: ::harmony_luau::AsyncCallFrame<'_>| {
                    #(#read_args)*
                    #body
                })
            }
        } else if returns_future {
            let body = scheduled_future_method_body(fn_name, &call_args, &fn_item.sig.output);
            quote! {
                .async_method(#fn_name_lit, [#(#method_argument_names),*], |__harmony_this: #self_ty, mut frame: ::harmony_luau::AsyncCallFrame<'_>| {
                    #(#read_args)*
                    #body
                })
            }
        } else {
            let body = sync_method_body(fn_name, &call_args, &fn_item.sig.output);
            quote! {
                .method(#fn_name_lit, [#(#method_argument_names),*], |__harmony_this: #self_ty, mut frame: ::harmony_luau::CallFrame<'_>| {
                    #(#read_args)*
                    #body
                })
            }
        };
        class_methods.push(registration);

        let params =
            match parameter_descriptors(&options, &fn_item.sig.inputs, fn_item.sig.ident.span()) {
                Ok(params) => params,
                Err(error) => return error.into_compile_error().into(),
            };
        let returns = return_descriptor_tokens(&options, &fn_item.sig.output);
        let yields = if is_async || returns_future {
            quote! { true }
        } else {
            quote! { false }
        };
        method_descriptors.push(quote! {
            ::harmony_luau::MethodDescriptor {
                name: #fn_name_lit,
                description: #description,
                params: vec![#(#params),*],
                returns: #returns,
                yields: #yields,
                kind: ::harmony_luau::MethodKind::Instance,
            }
        });

        let spec = method_spec_tokens(
            &options,
            &fn_item.sig.inputs,
            &fn_item.sig.output,
            &fn_name_lit,
            is_async || returns_future,
        );
        spec_methods.push(quote! {
            .method(#spec)
        });
    }

    quote! {
        #ast

        #[automatically_derived]
        impl #self_ty {
            #[doc(hidden)]
            pub fn _harmony_userdata_class() -> &'static ::harmony_core::UserDataClass<#self_ty> {
                static CLASS: ::std::sync::LazyLock<::harmony_core::UserDataClass<#self_ty>> =
                    ::std::sync::LazyLock::new(|| {
                        let class: ::harmony_core::UserDataClass<#self_ty> =
                            ::harmony_core::UserDataClass::new(
                            <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                        );
                        class
                            #(#class_methods)*
                    });
                &CLASS
            }

            #[doc(hidden)]
            pub fn _harmony_userdata_spec() -> ::harmony_core::UserDataSpec {
                ::harmony_core::UserDataSpec::new(
                    <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                )
                    #(#spec_methods)*
            }
        }

        #[automatically_derived]
        impl ::harmony_luau::DescribeUserData for #self_ty {
            fn class_descriptor() -> ::harmony_luau::ClassDescriptor {
                let mut descriptor = ::harmony_luau::ClassDescriptor::new(
                    <#self_ty as ::harmony_core::UserDataType>::CLASS_NAME,
                    <#self_ty as ::harmony_core::UserDataType>::DESCRIPTION,
                );
                descriptor.methods.extend(vec![#(#method_descriptors),*]);
                descriptor
            }
        }
    }
    .into()
}

fn parse_userdata_options(attr: TokenStream) -> syn::Result<UserDataOptions> {
    if attr.is_empty() {
        return Ok(UserDataOptions::default());
    }

    let options = syn::parse::<UserDataOptionList>(attr)?;
    let mut parsed = UserDataOptions::default();
    for option in options.0 {
        match option {
            UserDataOption::Name(value) => parsed.name = Some(value),
            UserDataOption::Description(value) => parsed.description = Some(value),
        }
    }
    Ok(parsed)
}

fn parse_harmony_method_options(attrs: &[Attribute]) -> syn::Result<HarmonyMethodOptions> {
    let mut options = HarmonyMethodOptions {
        skip: false,
        args: None,
        returns: None,
        description: None,
    };

    for attr in attrs {
        if !attr.path().is_ident("harmony") {
            continue;
        }

        let Meta::List(list) = &attr.meta else {
            return Err(syn::Error::new(
                attr.span(),
                "harmony helper attributes must use #[harmony(...)]",
            ));
        };
        let parsed = syn::parse2::<HarmonyMethodOptionList>(list.tokens.clone())?;

        for option in parsed.0 {
            match option {
                HarmonyMethodOption::Skip => options.skip = true,
                HarmonyMethodOption::Args(args) => {
                    options.args = Some(
                        args.0
                            .into_iter()
                            .map(|arg| DocParameterOverride {
                                name: arg.name.to_string(),
                                ty: arg.ty,
                            })
                            .collect(),
                    );
                }
                HarmonyMethodOption::Returns(returns) => {
                    options.returns = Some(returns.0.into_iter().collect());
                }
                HarmonyMethodOption::Description(description) => {
                    options.description = Some(description);
                }
            }
        }
    }

    Ok(options)
}

fn strip_helper_attributes(attrs: &mut Vec<Attribute>) {
    attrs.retain(|attr| !attr.path().is_ident("harmony"));
}

fn collect_parameters(inputs: &Punctuated<FnArg, Token![,]>) -> Vec<MethodParameter<'_>> {
    inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(pat_type) if !is_hidden_context_type(pat_type.ty.as_ref()) => {
                let Pat::Ident(pattern) = &*pat_type.pat else {
                    return None;
                };
                Some(MethodParameter {
                    ident: &pattern.ident,
                    ty: pat_type.ty.as_ref(),
                    name_lit: lit_str(&pattern.ident.to_string(), pattern.ident.span()),
                })
            }
            FnArg::Typed(_) | FnArg::Receiver(_) => None,
        })
        .collect()
}

fn collect_call_arguments(inputs: &Punctuated<FnArg, Token![,]>) -> Vec<TokenStream2> {
    inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(pat_type) if is_vm_context_type(pat_type.ty.as_ref()) => {
                Some(quote! { frame.vm })
            }
            FnArg::Typed(pat_type) if is_call_context_type(pat_type.ty.as_ref()) => {
                Some(quote! { &frame.context })
            }
            FnArg::Typed(pat_type) if is_chunk_origin_type(pat_type.ty.as_ref()) => {
                Some(quote! { &frame.context.origin })
            }
            FnArg::Typed(pat_type) => {
                let Pat::Ident(pattern) = &*pat_type.pat else {
                    return None;
                };
                let ident = &pattern.ident;
                Some(quote! { #ident })
            }
            FnArg::Receiver(_) => None,
        })
        .collect()
}

fn sync_method_body(
    fn_name: &Ident,
    call_args: &[TokenStream2],
    output: &ReturnType,
) -> TokenStream2 {
    let call = quote! { __harmony_this.#fn_name(#(#call_args),*) };
    match output {
        ReturnType::Default => quote! {
            #call;
            Ok(())
        },
        ReturnType::Type(_, ty) => {
            if let Some(inner) = extract_result_inner_type(ty) {
                if is_unit_type(&inner) {
                    quote! {
                        #call?;
                        Ok(())
                    }
                } else {
                    quote! {
                        let __harmony_result = #call?;
                        frame.returns.write(__harmony_result)
                    }
                }
            } else if is_unit_type(ty) {
                quote! {
                    #call;
                    Ok(())
                }
            } else {
                quote! {
                    let __harmony_result = #call;
                    frame.returns.write(__harmony_result)
                }
            }
        }
    }
}

fn async_method_body(
    fn_name: &Ident,
    call_args: &[TokenStream2],
    output: &ReturnType,
) -> TokenStream2 {
    let call = quote! { __harmony_this.#fn_name(#(#call_args),*).await };
    match output {
        ReturnType::Default => quote! {
            Ok(::harmony_luau::ScheduledFuture::new(async move {
                #call;
                Ok(())
            }))
        },
        ReturnType::Type(_, ty) => {
            if extract_result_inner_type(ty).is_some() {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        #call
                    }))
                }
            } else if is_unit_type(ty) {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        #call;
                        Ok(())
                    }))
                }
            } else {
                quote! {
                    Ok(::harmony_luau::ScheduledFuture::new(async move {
                        Ok(#call)
                    }))
                }
            }
        }
    }
}

fn scheduled_future_method_body(
    fn_name: &Ident,
    call_args: &[TokenStream2],
    output: &ReturnType,
) -> TokenStream2 {
    let call = quote! { __harmony_this.#fn_name(#(#call_args),*) };
    match output {
        ReturnType::Default => quote! {
            compile_error!("scheduled future userdata methods must return ScheduledFuture");
        },
        ReturnType::Type(_, ty) => {
            if extract_result_inner_type(ty).is_some() {
                quote! {
                    #call
                }
            } else {
                quote! {
                    Ok(#call)
                }
            }
        }
    }
}

fn parameter_descriptors(
    options: &HarmonyMethodOptions,
    inputs: &Punctuated<FnArg, Token![,]>,
    span: Span,
) -> syn::Result<Vec<TokenStream2>> {
    if let Some(args) = &options.args {
        return Ok(args
            .iter()
            .map(|arg| {
                let name = lit_str(&arg.name, span);
                let ty = &arg.ty;
                quote! {
                    ::harmony_luau::ParameterDescriptor {
                        name: #name,
                        ty: <#ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                        description: None,
                        variadic: false,
                    }
                }
            })
            .collect());
    }

    Ok(collect_parameters(inputs)
        .iter()
        .map(|param| {
            let name = &param.name_lit;
            let ty = param.ty;
            quote! {
                ::harmony_luau::ParameterDescriptor {
                    name: #name,
                    ty: <#ty as ::harmony_luau::LuauTypeInfo>::luau_type(),
                    description: None,
                    variadic: false,
                }
            }
        })
        .collect())
}

fn return_descriptor_tokens(options: &HarmonyMethodOptions, output: &ReturnType) -> TokenStream2 {
    if let Some(returns) = &options.returns {
        let luau_types = returns.iter().map(|ty| {
            quote! {
                <#ty as ::harmony_luau::LuauTypeInfo>::luau_type()
            }
        });
        return quote! {
            vec![#(#luau_types),*]
        };
    }

    if returns_scheduled_future(output) {
        return quote! {
            ::std::vec::Vec::new()
        };
    }

    match effective_return_type(output) {
        Some(return_ty) => quote! {
            <#return_ty as ::harmony_luau::LuauReturn>::luau_returns()
        },
        None => quote! {
            ::std::vec::Vec::new()
        },
    }
}

fn method_spec_tokens(
    options: &HarmonyMethodOptions,
    inputs: &Punctuated<FnArg, Token![,]>,
    output: &ReturnType,
    name: &LitStr,
    is_async: bool,
) -> TokenStream2 {
    let constructor = if is_async {
        quote! { async_fn }
    } else {
        quote! { sync_fn }
    };
    let mut spec = quote! {
        ::harmony_core::FunctionSpec::#constructor(#name)
    };

    let arg_types = if let Some(args) = &options.args {
        args.iter()
            .map(|arg| {
                let name = lit_str(&arg.name, name.span());
                let ty = &arg.ty;
                (name, quote! { #ty })
            })
            .collect::<Vec<_>>()
    } else {
        collect_parameters(inputs)
            .iter()
            .map(|param| {
                let name = &param.name_lit;
                let ty = param.ty;
                (name.clone(), quote! { #ty })
            })
            .collect::<Vec<_>>()
    };

    for (arg_name, arg_ty) in arg_types {
        spec = quote! {
            #spec.named_arg::<#arg_ty>(#arg_name)
        };
    }

    let return_types = if let Some(returns) = &options.returns {
        returns.iter().map(|ty| quote! { #ty }).collect::<Vec<_>>()
    } else {
        function_spec_return_types(output)
    };

    for return_ty in return_types {
        spec = quote! {
            #spec.returns::<#return_ty>()
        };
    }

    spec
}

fn function_spec_return_types(output: &ReturnType) -> Vec<TokenStream2> {
    let Some(return_ty) = effective_return_type(output) else {
        return Vec::new();
    };
    if is_unit_type(&return_ty) || is_scheduled_future_type(&return_ty) {
        return Vec::new();
    }
    if let Type::Tuple(tuple) = &return_ty {
        return tuple.elems.iter().map(|ty| quote! { #ty }).collect();
    }
    vec![quote! { #return_ty }]
}

fn effective_return_type(output: &ReturnType) -> Option<Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    extract_result_inner_type(ty).or_else(|| Some((**ty).clone()))
}

fn returns_scheduled_future(output: &ReturnType) -> bool {
    effective_return_type(output)
        .as_ref()
        .is_some_and(is_scheduled_future_type)
}

fn is_scheduled_future_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "ScheduledFuture")
}

fn extract_result_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }

    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };

    for argument in &arguments.args {
        if let syn::GenericArgument::Type(argument_ty) = argument {
            return Some(argument_ty.clone());
        }
    }

    None
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn has_hidden_context_parameter(inputs: &Punctuated<FnArg, Token![,]>) -> bool {
    inputs.iter().any(|argument| match argument {
        FnArg::Typed(pat_type) => is_hidden_context_type(pat_type.ty.as_ref()),
        FnArg::Receiver(_) => false,
    })
}

fn is_hidden_context_type(ty: &Type) -> bool {
    is_vm_context_type(ty) || is_call_context_type(ty) || is_chunk_origin_type(ty)
}

fn is_vm_context_type(ty: &Type) -> bool {
    is_type_named(ty, "Vm")
}

fn is_call_context_type(ty: &Type) -> bool {
    is_type_named(ty, "CallContext")
}

fn is_chunk_origin_type(ty: &Type) -> bool {
    is_type_named(ty, "ChunkOrigin")
}

fn is_type_named(ty: &Type, name: &str) -> bool {
    match ty {
        Type::Path(type_path) => is_type_path_named(type_path, name),
        Type::Reference(TypeReference { elem, .. }) => matches!(
            elem.as_ref(),
            Type::Path(type_path) if is_type_path_named(type_path, name)
        ),
        _ => false,
    }
}

fn is_type_path_named(type_path: &TypePath, name: &str) -> bool {
    type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident == name)
        .unwrap_or(false)
}

fn option_lit_str_tokens(value: Option<&LitStr>) -> TokenStream2 {
    match value {
        Some(value) => quote! { Some(#value) },
        None => quote! { None },
    }
}

fn lit_str(value: &str, span: Span) -> LitStr {
    LitStr::new(value.strip_prefix("r#").unwrap_or(value), span)
}
