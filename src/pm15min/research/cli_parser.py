from __future__ import annotations

import argparse

from pm15min.research.cli_args import (
    add_cycle_arg,
    add_market_arg,
    add_market_cycle_args,
    add_market_cycle_profile_args,
    add_profile_arg,
    add_target_arg,
)


def attach_research_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    def add_evaluation_run_args(parser: argparse.ArgumentParser) -> None:
        add_market_cycle_profile_args(parser)
        parser.add_argument("--scope", default="default")
        parser.add_argument("--run-label", default="planned")

    def add_poly_eval_routed_scope_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--method",
            default="is_auto",
            choices=[
                "crude",
                "antithetic",
                "stratified",
                "stratified_neyman",
                "is_exp_tilt",
                "is_auto",
                "cv_sv",
            ],
        )
        parser.add_argument("--seed", type=int, default=42)
        parser.add_argument("--input", default=None)
        parser.add_argument("--cols", default=None, help="Comma-separated variable columns, e.g. xrp_ret,sol_ret")
        parser.add_argument("--obs-col", default="obs")
        parser.add_argument("--n-paths", type=int, default=80_000)
        parser.add_argument("--n-steps", type=int, default=30)
        parser.add_argument("--step-mean", type=float, default=-0.03)
        parser.add_argument("--step-std", type=float, default=1.0)
        parser.add_argument("--start", type=float, default=0.0)
        parser.add_argument("--event-kind", choices=["terminal_cross", "last_n_comeback"], default="terminal_cross")
        parser.add_argument("--threshold", type=float, default=4.5)
        parser.add_argument("--direction", choices=["ge", "le"], default="ge")
        parser.add_argument("--lookback-steps", type=int, default=5)
        parser.add_argument("--min-deficit", type=float, default=-1.5)
        parser.add_argument("--recovery-level", type=float, default=0.0)
        parser.add_argument("--gamma", type=float, default=0.8)
        parser.add_argument("--target-hit-rate", type=float, default=0.15)
        parser.add_argument("--self-normalized-is", action="store_true")
        parser.add_argument("--n-strata", type=int, default=20)
        parser.add_argument("--pilot-paths-per-stratum", type=int, default=80)
        parser.add_argument("--vol-of-vol", type=float, default=0.35)
        parser.add_argument("--yes-ask", type=float, default=0.03)
        parser.add_argument("--no-ask", type=float, default=0.97)
        parser.add_argument("--fee-rate-entry", type=float, default=0.01)
        parser.add_argument("--fee-rate-exit", type=float, default=0.0)
        parser.add_argument("--half-spread", type=float, default=0.002)
        parser.add_argument("--extra-slippage", type=float, default=0.001)
        parser.add_argument("--min-ev", type=float, default=0.0)
        parser.add_argument("--min-roi", type=float, default=0.0)
        parser.add_argument("--n-particles", type=int, default=4000)
        parser.add_argument("--process-sigma", type=float, default=0.20)
        parser.add_argument("--obs-sigma", type=float, default=0.04)
        parser.add_argument("--resample-ess-ratio", type=float, default=0.5)
        parser.add_argument("--prior-yes-prob", type=float, default=0.5)
        parser.add_argument("--prior-logit-std", type=float, default=1.2)
        parser.add_argument("--synthetic-n", type=int, default=240)
        parser.add_argument("--synthetic-start-prob", type=float, default=0.5)
        parser.add_argument("--synthetic-process-sigma", type=float, default=0.10)
        parser.add_argument("--synthetic-obs-sigma", type=float, default=0.06)
        parser.add_argument("--family", choices=["gaussian", "t", "clayton", "gumbel"], default="t")
        parser.add_argument("--tail", choices=["lower", "upper"], default="lower")
        parser.add_argument("--quantile", type=float, default=0.05)
        parser.add_argument("--event-probs", default=None)
        parser.add_argument("--losses", default=None)
        parser.add_argument("--n-sim", type=int, default=200_000)
        parser.add_argument("--alpha", type=float, default=0.99)
        parser.add_argument("--tail-q", type=float, default=0.95)
        parser.add_argument("--nu", type=float, default=None)
        parser.add_argument("--nu-grid", default="2,3,4,6,8,12,20,30")
        parser.add_argument("--theta", type=float, default=None)
        parser.add_argument("--true-prob", type=float, default=0.62)
        parser.add_argument("--init-price", type=float, default=0.50)
        parser.add_argument("--n-informed", type=int, default=12)
        parser.add_argument("--n-noise", type=int, default=60)
        parser.add_argument("--n-mm", type=int, default=6)
        parser.add_argument("--copula-family", choices=["gaussian", "t", "clayton", "gumbel"], default="t")
        parser.add_argument("--copula-tail", choices=["lower", "upper"], default="lower")
        parser.add_argument("--copula-n-sim", type=int, default=120_000)
        parser.add_argument("--copula-quantile", type=float, default=0.05)
        parser.add_argument("--risk-alpha", type=float, default=0.99)
        parser.add_argument("--drawdown-alert", type=float, default=0.08)

    parser = subparsers.add_parser("research", help="Training, backtest, and evaluation domain.")
    research_sub = parser.add_subparsers(dest="research_command")

    show_config = research_sub.add_parser("show-config", help="Show the canonical research config.")
    add_market_cycle_profile_args(show_config)
    show_config.add_argument("--source-surface", default="backtest", choices=["live", "backtest"])
    show_config.add_argument("--feature-set", default="deep_otm_v1")
    show_config.add_argument("--label-set", default="truth")
    add_target_arg(show_config)
    show_config.add_argument("--model-family", default="deep_otm")
    show_config.add_argument("--run-prefix", default=None)

    show_layout = research_sub.add_parser("show-layout", help="Show the canonical research layout.")
    add_market_cycle_args(show_layout)

    list_runs = research_sub.add_parser("list-runs", help="List artifact runs for a market.")
    add_market_cycle_args(list_runs)
    list_runs.add_argument("--model-family", default=None)
    list_runs.add_argument("--target", default=None)
    list_runs.add_argument("--prefix", default=None)

    list_bundles = research_sub.add_parser("list-bundles", help="List model bundles for a market.")
    add_market_cycle_args(list_bundles)
    list_bundles.add_argument("--profile", default=None)
    list_bundles.add_argument("--target", default=None)
    list_bundles.add_argument("--prefix", default=None)

    show_active_bundle = research_sub.add_parser(
        "show-active-bundle",
        help="Show the single active bundle selection for a market/profile/target.",
    )
    add_market_cycle_args(show_active_bundle)
    add_profile_arg(show_active_bundle, default="deep_otm")
    add_target_arg(show_active_bundle)

    activate_bundle = research_sub.add_parser(
        "activate-bundle",
        help="Promote one bundle to the single active selection for a market/profile/target.",
    )
    add_market_cycle_args(activate_bundle)
    add_profile_arg(activate_bundle, default="deep_otm")
    add_target_arg(activate_bundle)
    activate_bundle.add_argument("--bundle-label", default=None)
    activate_bundle.add_argument("--notes", default=None)

    build = research_sub.add_parser("build", help="Build canonical research datasets.")
    build_sub = build.add_subparsers(dest="research_build_command")

    feature_frame = build_sub.add_parser("feature-frame", help="Plan the canonical feature-frame build.")
    add_market_cycle_profile_args(feature_frame)
    feature_frame.add_argument("--source-surface", default="backtest", choices=["live", "backtest"])
    feature_frame.add_argument("--feature-set", default="deep_otm_v1")

    label_frame = build_sub.add_parser("label-frame", help="Plan the canonical label-frame build.")
    add_market_cycle_profile_args(label_frame)
    label_frame.add_argument("--label-set", default="truth")

    training_set = build_sub.add_parser("training-set", help="Build the canonical training-set dataset.")
    add_market_cycle_profile_args(training_set)
    training_set.add_argument("--feature-set", default="deep_otm_v1")
    training_set.add_argument("--label-set", default="truth")
    add_target_arg(training_set)
    training_set.add_argument("--window-start", required=True)
    training_set.add_argument("--window-end", required=True)
    training_set.add_argument("--offset", type=int, required=True)

    train = research_sub.add_parser("train", help="Train research models.")
    train_sub = train.add_subparsers(dest="research_train_command")
    train_run = train_sub.add_parser("run", help="Plan a canonical training run.")
    add_market_cycle_profile_args(train_run)
    train_run.add_argument("--model-family", default="deep_otm")
    train_run.add_argument("--feature-set", default="deep_otm_v1")
    train_run.add_argument("--label-set", default="truth")
    add_target_arg(train_run)
    train_run.add_argument("--offsets", default="7,8,9")
    train_run.add_argument("--window-start", required=True)
    train_run.add_argument("--window-end", required=True)
    train_run.add_argument("--run-label", default="planned")
    train_run.add_argument("--parallel-workers", type=int, default=None)

    bundle = research_sub.add_parser("bundle", help="Build deployable model bundles.")
    bundle_sub = bundle.add_subparsers(dest="research_bundle_command")
    bundle_build = bundle_sub.add_parser("build", help="Build a canonical model bundle from a training run.")
    add_market_cycle_args(bundle_build)
    add_profile_arg(bundle_build, default="deep_otm")
    bundle_build.add_argument("--model-family", default="deep_otm")
    add_target_arg(bundle_build)
    bundle_build.add_argument("--offsets", default="7,8,9")
    bundle_build.add_argument("--bundle-label", default="planned")
    bundle_build.add_argument("--source-training-run", default=None)

    backtest = research_sub.add_parser("backtest", help="Replay research bundles offline.")
    backtest_sub = backtest.add_subparsers(dest="research_backtest_command")
    backtest_run = backtest_sub.add_parser("run", help="Run a minimal offline backtest from a model bundle.")
    add_market_cycle_args(backtest_run)
    add_profile_arg(backtest_run, default="deep_otm")
    add_target_arg(backtest_run)
    backtest_run.add_argument("--spec", default="baseline_truth")
    backtest_run.add_argument("--run-label", default="planned")
    backtest_run.add_argument("--bundle-label", default=None)
    backtest_run.add_argument("--stake-usd", type=float, default=None)
    backtest_run.add_argument("--max-notional-usd", type=float, default=None)
    backtest_run.add_argument("--secondary-bundle-label", default=None)
    backtest_run.add_argument("--fallback-reasons", default=None, help="Comma-separated hybrid fallback reasons.")
    backtest_run.add_argument("--parity-json", default=None, help="JSON mapping with backtest parity overrides.")

    experiment = research_sub.add_parser("experiment", help="Run research experiment suites.")
    experiment_sub = experiment.add_subparsers(dest="research_experiment_command")
    experiment_run = experiment_sub.add_parser("run-suite", help="Run a minimal experiment suite.")
    add_market_cycle_profile_args(experiment_run)
    experiment_run.add_argument("--suite", required=True)
    experiment_run.add_argument("--run-label", default="planned")

    evaluate = research_sub.add_parser("evaluate", help="Run evaluation and reporting flows.")
    evaluate_sub = evaluate.add_subparsers(dest="research_evaluate_command")
    for name in ("calibration", "drift", "poly-eval"):
        sub = evaluate_sub.add_parser(name, help=f"Plan a {name} evaluation run.")
        add_evaluation_run_args(sub)
        sub.add_argument("--backtest-spec", default="baseline_truth")
        sub.add_argument("--backtest-run-label", default=None)
        if name == "poly-eval":
            add_poly_eval_routed_scope_args(sub)

    deep_otm_demo = evaluate_sub.add_parser(
        "deep-otm-demo",
        help="Run the migrated deep OTM probability and decision demo under research evaluate.",
    )
    add_evaluation_run_args(deep_otm_demo)
    deep_otm_demo.add_argument("--seed", type=int, default=42)
    deep_otm_demo.add_argument(
        "--method",
        default="is_auto",
        choices=[
            "crude",
            "antithetic",
            "stratified",
            "stratified_neyman",
            "is_exp_tilt",
            "is_auto",
            "cv_sv",
        ],
    )
    deep_otm_demo.add_argument("--n-paths", type=int, default=80_000)
    deep_otm_demo.add_argument("--n-steps", type=int, default=30)
    deep_otm_demo.add_argument("--step-mean", type=float, default=-0.03)
    deep_otm_demo.add_argument("--step-std", type=float, default=1.0)
    deep_otm_demo.add_argument("--start", type=float, default=0.0)
    deep_otm_demo.add_argument("--event-kind", choices=["terminal_cross", "last_n_comeback"], default="terminal_cross")
    deep_otm_demo.add_argument("--threshold", type=float, default=4.5)
    deep_otm_demo.add_argument("--direction", choices=["ge", "le"], default="ge")
    deep_otm_demo.add_argument("--lookback-steps", type=int, default=5)
    deep_otm_demo.add_argument("--min-deficit", type=float, default=-1.5)
    deep_otm_demo.add_argument("--recovery-level", type=float, default=0.0)
    deep_otm_demo.add_argument("--gamma", type=float, default=0.8)
    deep_otm_demo.add_argument("--target-hit-rate", type=float, default=0.15)
    deep_otm_demo.add_argument("--self-normalized-is", action="store_true")
    deep_otm_demo.add_argument("--n-strata", type=int, default=20)
    deep_otm_demo.add_argument("--pilot-paths-per-stratum", type=int, default=80)
    deep_otm_demo.add_argument("--vol-of-vol", type=float, default=0.35)
    deep_otm_demo.add_argument("--yes-ask", type=float, default=0.03)
    deep_otm_demo.add_argument("--no-ask", type=float, default=0.97)
    deep_otm_demo.add_argument("--fee-rate-entry", type=float, default=0.01)
    deep_otm_demo.add_argument("--fee-rate-exit", type=float, default=0.0)
    deep_otm_demo.add_argument("--half-spread", type=float, default=0.002)
    deep_otm_demo.add_argument("--extra-slippage", type=float, default=0.001)
    deep_otm_demo.add_argument("--min-ev", type=float, default=0.0)
    deep_otm_demo.add_argument("--min-roi", type=float, default=0.0)

    smc_demo = evaluate_sub.add_parser(
        "smc-demo",
        help="Run the migrated particle filter demo under research evaluate.",
    )
    add_evaluation_run_args(smc_demo)
    smc_demo.add_argument("--input", default=None)
    smc_demo.add_argument("--obs-col", default="obs")
    smc_demo.add_argument("--seed", type=int, default=42)
    smc_demo.add_argument("--n-particles", type=int, default=4000)
    smc_demo.add_argument("--process-sigma", type=float, default=0.20)
    smc_demo.add_argument("--obs-sigma", type=float, default=0.04)
    smc_demo.add_argument("--resample-ess-ratio", type=float, default=0.5)
    smc_demo.add_argument("--prior-yes-prob", type=float, default=0.5)
    smc_demo.add_argument("--prior-logit-std", type=float, default=1.2)
    smc_demo.add_argument("--synthetic-n", type=int, default=240)
    smc_demo.add_argument("--synthetic-start-prob", type=float, default=0.5)
    smc_demo.add_argument("--synthetic-process-sigma", type=float, default=0.10)
    smc_demo.add_argument("--synthetic-obs-sigma", type=float, default=0.06)

    copula_risk = evaluate_sub.add_parser(
        "copula-risk",
        help="Run the migrated copula tail-risk evaluation on a CSV input.",
    )
    add_evaluation_run_args(copula_risk)
    copula_risk.add_argument("--input", required=True)
    copula_risk.add_argument("--cols", required=True, help="Comma-separated variable columns, e.g. xrp_ret,sol_ret")
    copula_risk.add_argument("--family", choices=["gaussian", "t", "clayton", "gumbel"], default="t")
    copula_risk.add_argument("--tail", choices=["lower", "upper"], default="lower")
    copula_risk.add_argument("--quantile", type=float, default=0.05)
    copula_risk.add_argument("--event-probs", default=None)
    copula_risk.add_argument("--losses", default=None)
    copula_risk.add_argument("--n-sim", type=int, default=200_000)
    copula_risk.add_argument("--alpha", type=float, default=0.99)
    copula_risk.add_argument("--tail-q", type=float, default=0.95)
    copula_risk.add_argument("--nu", type=float, default=None)
    copula_risk.add_argument("--nu-grid", default="2,3,4,6,8,12,20,30")
    copula_risk.add_argument("--theta", type=float, default=None)
    copula_risk.add_argument("--seed", type=int, default=42)

    stack_demo = evaluate_sub.add_parser(
        "stack-demo",
        help="Run the migrated production stack demo under research evaluate.",
    )
    add_evaluation_run_args(stack_demo)
    stack_demo.add_argument("--seed", type=int, default=42)
    stack_demo.add_argument("--n-steps", type=int, default=1200)
    stack_demo.add_argument("--true-prob", type=float, default=0.62)
    stack_demo.add_argument("--init-price", type=float, default=0.50)
    stack_demo.add_argument("--n-informed", type=int, default=12)
    stack_demo.add_argument("--n-noise", type=int, default=60)
    stack_demo.add_argument("--n-mm", type=int, default=6)
    stack_demo.add_argument("--n-particles", type=int, default=3000)
    stack_demo.add_argument("--process-sigma", type=float, default=0.12)
    stack_demo.add_argument("--obs-sigma", type=float, default=0.05)
    stack_demo.add_argument("--copula-family", choices=["gaussian", "t", "clayton", "gumbel"], default="t")
    stack_demo.add_argument("--copula-tail", choices=["lower", "upper"], default="lower")
    stack_demo.add_argument("--copula-n-sim", type=int, default=120_000)
    stack_demo.add_argument("--copula-quantile", type=float, default=0.05)
    stack_demo.add_argument("--risk-alpha", type=float, default=0.99)
    stack_demo.add_argument("--drawdown-alert", type=float, default=0.08)
