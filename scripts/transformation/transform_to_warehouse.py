from scripts.transformation.load_warehouse import main


if __name__ == "__main__":
    try:
        results = main()
        print(results)
        print("\nPHASE 3 COMPLETE: Warehouse is ready.")
    except Exception as exc:
        print(f"Error: {exc}")