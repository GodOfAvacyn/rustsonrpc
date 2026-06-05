use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::quote;
use syn::{
    parse_macro_input, Error, FnArg, ImplItem, ImplItemFn, ItemImpl, Lit, LitStr, Meta,
    MetaNameValue, Pat, PatIdent, ReturnType, Type,
};

/// Marks an inherent impl block whose `#[rpc_method]` functions should be
/// exposed as JSON-RPC methods.
///
/// Generates an `impl Service` for the type with two members:
///   * `methods()` — the list of exposed method names; each name's position in
///     the slice is its method id.
///   * `dispatch(method_id, params)` — a `match` over the ids that decodes
///     params and calls the corresponding inherent method.
///
/// The service itself is never flattened into closures; a peer keeps it alive
/// as an `Arc<dyn Service>` and routes calls into `dispatch`.
#[proc_macro_attribute]
pub fn rpc_service(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut item_impl = parse_macro_input!(item as ItemImpl);
    let service_type = item_impl.self_ty.clone();

    let mut method_names: Vec<LitStr> = Vec::new();
    let mut dispatch_arms: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut errors: Vec<Error> = Vec::new();
    let mut index: u32 = 0;

    for item in &mut item_impl.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };

        let Some(attr_index) = method
            .attrs
            .iter()
            .position(|attr| attr.path().is_ident("rpc_method"))
        else {
            continue;
        };

        let attr = method.attrs.remove(attr_index);
        let method_name = match rpc_method_name(&attr.meta, method) {
            Ok(name) => name,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };

        match dispatch_arm(method, index) {
            Ok(arm) => {
                method_names.push(LitStr::new(&method_name, method.sig.ident.span()));
                dispatch_arms.push(arm);
                index += 1;
            }
            Err(error) => errors.push(error),
        }
    }

    let errors = errors.iter().map(Error::to_compile_error);

    quote! {
        #item_impl

        #[::rustsonrpc::__async_trait::async_trait]
        impl ::rustsonrpc::service::Service for #service_type {
            fn methods(&self) -> &'static [&'static str] {
                &[#(#method_names),*]
            }

            #[allow(unused_variables)]
            async fn dispatch(
                &self,
                method: ::core::primitive::u32,
                params: ::rustsonrpc::params::DynamicParams,
            ) -> ::rustsonrpc::errors::JsonRpcResult<::rustsonrpc::__serde_json::Value> {
                match method {
                    #(#dispatch_arms)*
                    _ => ::core::result::Result::Err(
                        ::rustsonrpc::errors::JsonRpcError::method_not_found(),
                    ),
                }
            }
        }

        #(#errors)*
    }
    .into()
}

/// Marks a method inside an `#[rpc_service]` impl block for RPC exposure.
#[proc_macro_attribute]
pub fn rpc_method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

fn rpc_method_name(meta: &Meta, method: &ImplItemFn) -> syn::Result<String> {
    match meta {
        Meta::Path(_) => Ok(method.sig.ident.to_string()),
        Meta::List(list) => {
            if let Ok(name) = list.parse_args::<LitStr>() {
                return Ok(name.value());
            }

            let name = list.parse_args::<MetaNameValue>()?;
            if !name.path.is_ident("name") {
                return Err(Error::new_spanned(name.path, "expected `name = \"...\"`"));
            }

            match name.value {
                syn::Expr::Lit(expr) => match expr.lit {
                    Lit::Str(value) => Ok(value.value()),
                    other => Err(Error::new_spanned(other, "expected string literal")),
                },
                other => Err(Error::new_spanned(other, "expected string literal")),
            }
        }
        Meta::NameValue(name) => Err(Error::new_spanned(
            name,
            "expected `#[rpc_method]`, `#[rpc_method(\"name\")]`, or `#[rpc_method(name = \"name\")]`",
        )),
    }
}

/// Builds a single `match` arm for `dispatch`: `idx => { decode params; call; serialize }`.
fn dispatch_arm(method: &ImplItemFn, index: u32) -> syn::Result<proc_macro2::TokenStream> {
    if !matches!(method.sig.inputs.first(), Some(FnArg::Receiver(_))) {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "rpc methods must take `&self` as their first parameter",
        ));
    }

    let method_ident = &method.sig.ident;
    let await_call = method.sig.asyncness.is_some();
    let mut arg_extractors = Vec::new();
    let mut arg_names = Vec::new();

    for input in &method.sig.inputs {
        let FnArg::Typed(arg) = input else {
            continue;
        };

        let Pat::Ident(PatIdent { ident, .. }) = arg.pat.as_ref() else {
            return Err(Error::new_spanned(
                &arg.pat,
                "rpc method params must be simple named arguments",
            ));
        };

        let ty: &Type = arg.ty.as_ref();
        let key = LitStr::new(&ident.to_string(), ident.span());
        arg_names.push(ident.clone());
        arg_extractors.push(quote! {
            let #ident: #ty = params.get(#key)?;
        });
    }

    if matches!(method.sig.output, ReturnType::Default) {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "rpc methods must return `JsonRpcResult<T>`",
        ));
    }

    let call = if await_call {
        quote! { self.#method_ident(#(#arg_names),*).await }
    } else {
        quote! { self.#method_ident(#(#arg_names),*) }
    };

    let index_lit = Literal::u32_suffixed(index);

    Ok(quote! {
        #index_lit => {
            #(#arg_extractors)*

            let result = #call?;

            ::rustsonrpc::__serde_json::to_value(result).map_err(|error| {
                ::rustsonrpc::errors::JsonRpcError::internal_error(format!(
                    "failed to serialize RPC result: {error}"
                ))
            })
        }
    })
}
