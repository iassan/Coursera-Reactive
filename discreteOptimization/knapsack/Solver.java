import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The class <code>Solver</code> is an implementation of a greedy algorithm to solve the knapsack problem.
 */
public class Solver {

	/**
	 * The main class
	 */
	public static void main(String[] args) {
		try {
			Solver solver = new Solver();
			Problem problem = solver.loadProblem(args);
			Solution solution = solver.solveDynamicProgrammingWithStaticO(problem);
			solver.outputSolution(solution);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Solution solveDynamicProgrammingWithStaticO(Problem problem) {
		int[][] o = new int[problem.capacity + 1][problem.itemsNo + 1]; // O(K, j)
		for (int k = 0; k <= problem.capacity; k++) {
			o[k][0] = 0;
		}
		for (int j = 1; j <= problem.itemsNo; j++) {
			int currWeight = problem.weights[j - 1];
			int currValue = problem.values[j - 1];
			for (int k = 0; k <= problem.capacity; k++) {
				if (k < currWeight) {
					o[k][j] = o[k][j - 1];
				} else {
					o[k][j] = Math.max(o[k][j - 1], o[k - currWeight][j - 1] + currValue);
				}
			}
		}
		return findSolutionFromStaticO(o, problem);
	}

	private Solution findSolutionFromStaticO(int[][] o, Problem problem) {
		int[] taken = new int[problem.itemsNo];
		int k = problem.capacity;
		for (int j = problem.itemsNo; j > 0; j--) {
			if (o[k][j] == o[k][j - 1]) {
				taken[j - 1] = 0;
			} else {
				taken[j - 1] = 1;
				k = k - problem.weights[j - 1];
			}
		}

		return new Solution(o[problem.capacity][problem.itemsNo], taken);
	}

	public Solution solveTrivial(Problem problem) {

		// a trivial greedy algorithm for filling the knapsack
		// it takes items in-order until the knapsack is full
		int value = 0;
		int weight = 0;
		int[] taken = new int[problem.getItemsNo()];

		for (int i = 0; i < problem.getItemsNo(); i++) {
			if (weight + problem.getWeights()[i] <= problem.getCapacity()) {
				taken[i] = 1;
				value += problem.getValues()[i];
				weight += problem.getWeights()[i];
			} else {
				taken[i] = 0;
			}
		}
		return new Solution(value, taken);
	}

	private Problem loadProblem(String[] args) throws IOException {
		String fileName = null;

		// get the temp file name
		for (String arg : args) {
			if (arg.startsWith("-file=")) {
				fileName = arg.substring(6);
			}
		}
		if (fileName == null) {
			throw new FileNotFoundException("No file requested!");
		}

		// read the lines out of the file
		List<String> lines = new ArrayList<String>();

		BufferedReader input = new BufferedReader(new FileReader(fileName));
		try {
			String line;
			while ((line = input.readLine()) != null) {
				lines.add(line);
			}
		} finally {
			input.close();
		}

		// parse the data in the file
		String[] firstLine = lines.get(0).split("\\s+");
		int items = Integer.parseInt(firstLine[0]);

		int[] values = new int[items];
		int[] weights = new int[items];

		for (int i = 1; i < items + 1; i++) {
			String line = lines.get(i);
			String[] parts = line.split("\\s+");

			values[i - 1] = Integer.parseInt(parts[0]);
			weights[i - 1] = Integer.parseInt(parts[1]);
		}

		Problem problem = new Problem();
		problem.setItemsNo(Integer.parseInt(firstLine[0]));
		problem.setCapacity(Integer.parseInt(firstLine[1]));
		problem.setValues(values);
		problem.setWeights(weights);
		return problem;
	}

	private void outputSolution(Solution solution) {
		// prepare the solution in the specified output format
		System.out.println(solution.getValue() + " 0");
		for (int i : solution.getTaken()) {
			System.out.print(i + " ");
		}
		System.out.println("");
	}

	private static class Problem {

		int itemsNo;

		int capacity;

		int[] values;

		int[] weights;

		public int getItemsNo() {
			return itemsNo;
		}

		public void setItemsNo(int itemsNo) {
			this.itemsNo = itemsNo;
		}

		public int getCapacity() {
			return capacity;
		}

		public void setCapacity(int capacity) {
			this.capacity = capacity;
		}

		public int[] getValues() {
			return values;
		}

		public void setValues(int[] values) {
			this.values = values;
		}

		public int[] getWeights() {
			return weights;
		}

		public void setWeights(int[] weights) {
			this.weights = weights;
		}
	}

	private static class Solution {

		int value;

		int[] taken;

		private Solution(int value, int[] taken) {
			this.value = value;
			this.taken = taken;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int[] getTaken() {
			return taken;
		}

		public void setTaken(int[] taken) {
			this.taken = taken;
		}
	}
}
